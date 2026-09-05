# Cutting a Freenet release

> **The AWS gateway was retired in September 2026.** References to it below that
> remain are historical incident records (notably the v0.2.71 half-applied
> rollout that motivated the post-deploy verify) and are kept deliberately —
> they explain why a check exists. It is no longer a rollout target: the release
> matrix, the manual SSH driver in `release.sh`, and `RELEASE_AGENT_HMAC_VEGA`
> have all been removed. nova now runs two gateway processes; the second,
> `freenet-gateway-2`, has no release-agent of its own and is brought up by the
> primary's stop/start cycle, with `gateway-auto-update.sh` verifying it via the
> `.wants` symlinks so a companion that fails to start is not reported as a
> successful update.


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
   `verify_publishable` → `create_release` jobs complete.
2. An auto-created bump PR titled `build: release X.Y.Z` that merges itself.
3. A `vX.Y.Z` git tag pushed and a **draft** GitHub release created, which
   triggers `Build and Cross-Compile`.
4. Cross-compile builds Linux musl + macOS (Intel + arm64) + Windows
   (Authenticode-signed) + signed DMG and attaches all 14 artifacts to the
   draft release.
5. Still inside that job, in this order: **Gate A** (the blocking auto-update
   pre-flight, see "Release gates" below) → `freenet` and `fdev` published to
   crates.io → undraft.
6. The undraft fires `release.published` → `Gateway Update` and
   `Release Announcements` both auto-trigger.
7. nova's gateways converge to the new version (verified by the
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
| `CARGO_REGISTRY_TOKEN` | release.yml `validate` (checks it), cross-compile.yml `attach-to-release` (uses it) | **`validate` fails first**, before the bump PR exists — that is the intended place to find out, and where to look when a release dies immediately. If `validate` is bypassed (a bare tag push, or a manual `cross-compile.yml` dispatch), `attach-to-release` fails after the binaries are uploaded and the release stays a draft. The actual publish lives in cross-compile.yml, not release.yml, because it is deliberately downstream of Gate A — see "The crates.io publish is downstream of Gate A" below. |
| `MATRIX_HOMESERVER_URL` | release-announce.yml | Matrix job warns + skips (success, no post). |
| `MATRIX_ACCESS_TOKEN` | release-announce.yml | Matrix job warns + skips. |
| `RELEASE_AGENT_HMAC_NOVA` | gateway-update.yml, release-announce.yml | nova update + River announce fail (HTTP 401). |
| `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID` | cross-compile.yml `build-x86_64-windows` (Authenticode signing) | **Does NOT degrade gracefully — this one fails the release.** See "Windows code signing" below. |
| `FREENET_RELEASE_SIGNING_KEY` | cross-compile.yml (`Sign SHA256SUMS.txt`) | Releases are UNSIGNED (no `SHA256SUMS.txt.sig` attached). Clients accept unsigned releases during the transition window (`REQUIRE_RELEASE_SIGNATURE = false`), but once that flag is flipped to `true` in a future release, unsigned releases are REFUSED by the auto-updater. PEM-encoded ed25519 private key whose public half is baked into the updater (`update.rs` `FREENET_RELEASE_PUBKEY`). |

To validate `FREENET_RELEASE_SIGNING_KEY` without cutting a release, run the
`Build and Cross-Compile` workflow via `workflow_dispatch`: its
`verify-signing-key` job derives the public key from the secret, asserts it
matches the key baked into the binary, and does a sign/verify round-trip.

### Windows code signing (Authenticode)

`freenet.exe` and `fdev.exe` are Authenticode-signed in
`build-x86_64-windows` via [Azure Artifact
Signing](https://learn.microsoft.com/azure/artifact-signing/), on release tags
and on manual `workflow_dispatch` only (routine main-push builds are not
signed, and can never become a release because `attach-to-release` is
tag-gated). Signing sits between the smoke test and the `upload-artifact`
steps, so `SHA256SUMS.txt` — generated later in `attach-to-release` from the
downloaded artifacts — covers the signed bytes.

Signing `freenet.exe` covers the installer, uninstaller, tray, service wrapper
and updater at once: on Windows they are all the same self-contained binary.
`fdev.exe` is signed because `installer::run_install` downloads it from the
GitHub release at install time.

Expected signer subject:

```
CN=Freenet Project Inc, O=Freenet Project Inc, L=Austin, S=Texas, C=US
```

**Unlike every other secret in the table above, this path is fail-closed.** The
`Verify signatures` step runs `Get-AuthenticodeSignature` on the runner and
throws if a binary is unsigned, invalid, missing its RFC3161 timestamp, or
**signed by a publisher other than `CN=Freenet Project Inc`**. That last check
matters because `Valid` on its own only means "chains to a trusted root and is
timestamped" — it says nothing about who signed it, so without an explicit
subject assertion a binary signed by a different certificate profile would pass. A
broken Azure configuration therefore fails `build-x86_64-windows`, and because
`attach-to-release` needs that job, the release stops as a draft rather than
shipping unsigned binaries. That is deliberate — but it means Azure-side
breakage is a release-blocking failure, not a warning.

Authentication is OIDC federation (`azure/login@v3`), so there is no client
secret and no exportable key: the Artifact Signing key lives in Microsoft's
HSM and never leaves it. The three `AZURE_*` values are identifiers, stored as
secrets only by convention. The federated credential in Entra is pinned to the
subject `repo:freenet/freenet-core:environment:release`, which is why the job
declares `environment: release` and `permissions: id-token: write`. **Those
three lines are load-bearing for authentication — do not "simplify" them.**

Note that adding required reviewers to the `release` environment would pause
*every* `Build and Cross-Compile` run, including main-push builds, since the
Windows job is not itself gated to tags.

The RFC3161 timestamp is not optional: Artifact Signing certificates are
short-lived (~3 days), so without a countersigned timestamp every release
binary would stop validating almost immediately.

To verify a published asset from Linux or macOS (no Windows machine needed).
Fetch the Microsoft root and verify against it — that root alone is enough, so
this works the same on both platforms:

```bash
# Linux: apt install osslsigncode    macOS: brew install osslsigncode
curl -o msroot.crt \
  "https://www.microsoft.com/pkiops/certs/microsoft%20identity%20verification%20root%20certificate%20authority%202020.crt"
openssl x509 -inform DER -in msroot.crt -out msroot.pem

osslsigncode verify -in freenet.exe -CAfile msroot.pem -TSA-CAfile msroot.pem
```

Expect, on a good binary:

```
Current message digest    : <hash>
Calculated message digest : <hash>      # must MATCH — this is the integrity check
Signature verification: ok
Timestamp Server Signature verification: ok
Number of verified signatures: 1
Succeeded
```

plus the signer subject above.

**Pass `-TSA-CAfile` as well as `-CAfile`, and do not skip it.** With `-CAfile`
alone the command still prints `Succeeded` and exits 0 — but the timestamp
chain was NOT verified, and the only sign of that is a
`Timestamp Server Signature verification: failed` line further up the output.
Given the three-day signing certificate below, the countersignature is the part
that matters most, so a check that silently skips it is close to no check at
all. Measured behaviour against a real signed artifact:

| Invocation | Signature | Timestamp | Prints |
|---|---|---|---|
| no CA arguments | failed | failed | `Failed` (exit 1) |
| `-CAfile msroot.pem` | ok | **failed** | `Succeeded` (exit 0) |
| `-CAfile msroot.pem -TSA-CAfile msroot.pem` | ok | ok | `Succeeded` (exit 0) |

The bare form fails because the Microsoft Identity Verification Root CA 2020 is
not in a typical Linux or macOS trust store, so no chain can be built. That is
a local trust-store artifact, not a problem with the signature — but it means
**a bare `osslsigncode verify` reports `failed` on a perfectly good binary**,
which is exactly the wrong impression to give someone checking a release.

**The signing certificate is valid for about 3 days** (a real example:
`notBefore Aug 24 15:22:28 2026`, `notAfter Aug 27 15:22:28 2026`). That is
normal for Artifact Signing and is exactly why the RFC3161 countersignature is
mandatory — the timestamp is what keeps already-shipped binaries validating
after the certificate expires.

SmartScreen reputation is per-publisher and accrues over downloads, so expect
the download warning to soften rather than vanish on the first signed release.
Observing that requires a Windows machine and does not gate anything.

`RELEASE_PAT` is a personal access token with `repo` (Contents, Pull
requests, Metadata) and `workflow` scopes. See AGENTS.md → "Release Workflow
& RELEASE_PAT" for the full rationale (GITHUB_TOKEN suppresses
workflow-triggering events as an anti-recursion safeguard, so PAT is
required for the cascade to fire automatically).

### Wire-gated feature floors (one-time, per feature)

Some features that add a new wire variant are version-gated: a node only sends
the new variant to peers whose negotiated protocol version is at or above a
hardcoded floor, so older peers never receive a variant they can't deserialize
(they'd drop the connection). Most such variants are added to `NetMessageV1`,
but **not all** — `GATEWAY_ACK_VERSION_MIN_VERSION` below gates a
`SymmetricMessagePayload` variant on the transport handshake instead, so do not
scan for `NetMessageV1` alone when checking which floors apply. When you cut the release that
**first** ships such a feature, set its floor to **exactly that release
version**, then leave it frozen — do NOT bump it on later releases (raising it
above the first-shipping version would silently stop sending to fully-capable
peers).

Current wire-gated floors:

- `SUBSCRIBE_HINT_MIN_VERSION` in `crates/core/src/node/network_bridge/p2p_protoc.rs`
  (SubscribeHint placement migration, #4404).

  Set to **`(0, 2, 80)`** and FROZEN. 0.2.80 is the first release that ships
  SubscribeHint together with the #4145 event-loop fix (#4499) that makes the
  migration load-safe, so this floor now follows the general "set to the
  first-shipping release version, then freeze" rule above. The SEND gate emits a
  hint only to peers reported at `>= 0.2.80`; the inbound-hint RECEIVE gate (in
  `node.rs`) acts on a hint only if THIS node is itself `>= 0.2.80` (a proxy: the
  per-connection sender version is not exposed at the receive handler, so the
  load-bearing receiver gates on its own version, which guarantees the node that
  takes on migration load always has the #4145 fix). Because pre-0.2.80 releases
  have parked floors and emit no hints, in practice hints flow only between
  `>= 0.2.80` peers, so activation ramps with fleet upgrade rather than switching
  on everywhere at once. **DO NOT bump this floor on later releases** (raising it
  above the first-shipping version would silently stop sending to fully-capable
  peers).

  History: the migration first shipped in v0.2.73 and its directed-subscribe /
  hint-broadcast load drove a network-wide UPDATE-broadcast degradation by
  amplifying the latent #4145 event-loop wedge. v0.2.74 disabled it by parking
  this floor at `(0, 3, 0)`, above all live peers. It was re-enabled at
  `(0, 2, 80)` only after #4145 was fixed (#4499), the fix was validated at
  incident-scale fan-out, and the broadcast-assembly-failure telemetry (#4498)
  was deployed to record a baseline and watch the rollout.

- `SUMMARY_FIRST_PUT_MIN_VERSION` in the same file (summary-first PUT
  probe/dispatch variants, #4642 step 3-bis).

  Set to **`(0, 2, 95)`** and FROZEN — the release that first shipped the
  `PutMsg::ProbeRequest` / `ProbeResponse` / `ProbeReconcile` variants together
  with their handler.

- `HASH_FIRST_SUMMARIES_MIN_VERSION` in the same file (hash-first InterestSync
  summary exchange, #4965).

  Set to **`(0, 2, 116)`**, the release intended to first ship the
  `InterestMessage::SummaryDigests` / `SummaryRequest` variants and their
  handlers.

  **This one is guarded by a marker, not by a manual check.** Alongside the
  floor there is `HASH_FIRST_SHIPPED_IN: Option<(u8, u8, u16)>` (now
  `Some((0, 2, 116))` — the feature shipped; this paragraph previously said
  "currently `None`" and had gone stale). The test
  `connection_manager.rs::hash_first_floor_tracks_the_shipping_release`
  asserts:

  - while the marker is `None`, the floor must stay **strictly above**
    `CARGO_PKG_VERSION`;
  - once it is `Some(v)`, `v` must **equal** the floor and be at or below the
    crate version.

  So the moment a release bump raises `Cargo.toml` to the floor's value, that
  test fails and the releaser must consciously choose:

  - **this release carries hash-first** → set
    `HASH_FIRST_SHIPPED_IN = Some(HASH_FIRST_SUMMARIES_MIN_VERSION)` and freeze
    both; or
  - **it does not** → raise `HASH_FIRST_SUMMARIES_MIN_VERSION` to the release
    that will.

  Why a marker rather than the manual check used above: at this project's
  cadence (five releases in four days at the time of writing) a floor naming
  "the next release" goes stale silently. If 0.2.116 ships *without* the
  feature, peers on the real 0.2.116 satisfy the floor while carrying no
  `SummaryDigests` variant index — they cannot decode it, and the connection is
  closed, presenting as fleet-wide transport churn during the 0-4h staggered
  rollout. The marker makes that state fail a test instead of reaching users.

  `hash_first_floor_stays_above_every_release_without_the_variants` is kept as
  a companion: it catches the floor being *lowered*, which the marker test does
  not.

- `GATEWAY_ACK_VERSION_MIN_VERSION` in
  `crates/core/src/transport/connection_handler/version_cmp.rs` — **a different
  file and a different layer from the three above** (version-carrying connection
  ack, #5161).

  Set to **`(0, 2, 120)`**, the release intended to first ship the
  `SymmetricMessagePayload::AckConnectionV2` variant.

  **Its failure mode is more severe than the others', so bias high if in
  doubt.** The three floors above gate application messages: a peer that cannot
  decode one drops the connection. This one gates the connection ACK itself, so
  a peer that cannot decode it never completes the handshake at all — a floor
  set too low means every pre-floor node fails to connect to any upgraded
  gateway, and the release cascade upgrades the gateways FIRST.

  Guarded by a marker exactly like `HASH_FIRST_SHIPPED_IN`:
  `ACK_VERSION_SHIPPED_IN: Option<(u8, u8, u16)>`, currently `None`, checked by
  `version_cmp.rs::ack_version_floor_tracks_the_shipping_release`. When a
  release bump raises `CARGO_PKG_VERSION` to `(0, 2, 120)`, that test fails
  until the releaser consciously either sets
  `ACK_VERSION_SHIPPED_IN = Some(GATEWAY_ACK_VERSION_MIN_VERSION)` (this release
  carries it) or raises the floor (it does not).

  Note the emission gate reads the peer's version from the intro packet it just
  parsed, never from a cached value, so unlike the floors above there is no
  bootstrapping round-trip and no way for the decision to go stale against a
  peer that downgraded at a reused address.

- `BROADCAST_TARGET_LIST_MIN_VERSION` in
  `crates/core/src/node/network_bridge/p2p_protoc.rs` — the originator target
  list on contract broadcasts (#5147).

  Set to **`(0, 2, 120)`**, the release intended to first ship the
  `UpdateMsg::BroadcastToV2` / `BroadcastToStreamingV2` variants.

  Same failure mode as the three application-message floors above, and it is
  worth stating concretely because it is easy to read as merely a lost
  optimisation: a peer at or above the floor that does **not** carry the code
  receives a variant index it has no arm for, `decode_msg` fails, and the
  connection is CLOSED rather than degraded. Across the 0-4h staggered rollout
  that presents as fleet-wide transport churn, not as a feature that quietly
  did nothing.

  Guarded by a marker exactly like `HASH_FIRST_SHIPPED_IN`:
  `BROADCAST_TARGET_LIST_SHIPPED_IN: Option<(u8, u8, u16)>`, currently `None`,
  checked by
  `connection_manager.rs::broadcast_target_list_floor_tracks_the_shipping_release`.
  When a release bump raises `CARGO_PKG_VERSION` to `(0, 2, 120)`, that test
  fails until the releaser consciously either sets
  `BROADCAST_TARGET_LIST_SHIPPED_IN = Some(BROADCAST_TARGET_LIST_MIN_VERSION)`
  (this release carries it) or raises the floor (it does not).
  `broadcast_target_list_floor_stays_above_every_release_without_the_variants`
  is the companion that catches the floor being *lowered*.

  **Know the one way to resolve that red test wrongly.** The `Some(..)` arm
  asserts `shipped == floor && floor <= current`. Setting the marker to
  `Some((0, 2, 120))` when the code did NOT land in 0.2.120 satisfies both
  clauses and goes green — so the marker records a claim no test can check.
  If the feature slips to a later release, RAISE THE FLOOR; do not set the
  marker to make the test pass.

  **This floor DEPENDS on `GATEWAY_ACK_VERSION_MIN_VERSION` above and shares
  its target release.** Until the version-carrying ack ships, a node never
  learns its gateway's version, so this gate fails closed on every gateway link
  no matter what the gateway actually runs. If the two are ever separated, this
  one must not ship BEFORE that one, or it is inert on exactly the links that
  carry the most fan-out.

  Note also that a mis-set floor here is not symmetric with the others. Too low
  closes connections, as above. Too high does not merely lose an optimisation
  either — it is the *suppression* side that carries the risk, because a
  wrongly-suppressed peer waits for the ~5-minute interest heartbeat. Bias
  high, but understand that neither direction is free.

When a NEW wire-gated feature first ships (not this one), set its floor to
**exactly that release version** and freeze it, as described above.

## What fires when

```
gh workflow run release.yml
    └─→ release.yml: validate
    └─→ release.yml: update_versions
            └─→ creates "build: release X.Y.Z" PR
            └─→ ci.yml runs on PR (using RELEASE_PAT scope)
            └─→ PR auto-merges to main
    └─→ release.yml: wait_for_pr
            └─→ resolves the bump PR's merge commit -> RELEASE_SHA
                (everything below checks out that exact commit; see #5233)
    └─→ release.yml: verify_publishable
            └─→ cargo publish -p freenet --dry-run   (packaging check only —
                nothing is uploaded here; see the section below)
    └─→ release.yml: create_release
            └─→ git tag -a vX.Y.Z; git push
                    └─→ tag push triggers cross-compile.yml
            └─→ gh release create --draft (release exists but draft)
    cross-compile.yml: matrix builds + DMG sign/notarize
    cross-compile.yml: attach-to-release
            └─→ uploads 14 artifacts
            └─→ Gate A: auto-update pre-flight   ← BLOCKING
            └─→ cargo publish freenet            ← IRREVERSIBLE, and the first
            └─→ cargo publish fdev                 irreversible step in the run
            └─→ gh release edit --draft=false  ← uses RELEASE_PAT
                    └─→ fires release.published event
                            └─→ gateway-update.yml fires
                                    └─→ POST /update to nova (HTTPS)
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
state and proceed to `verify_publishable`.

### `verify_publishable` failed

This job only runs `cargo publish -p freenet --dry-run`, so a failure here is a
packaging problem, not a registry one: most often an `include_str!` /
`include_bytes!` path pointing outside the crate (#4240), which `cargo publish`
catches and an ordinary `cargo build` does not. Nothing has been uploaded and
no tag exists yet. Fix it on `main` and re-run the release.

### Known gap: `fdev` has no packaging pre-flight

`verify_publishable` checks **`freenet` only**. `fdev` is not dry-run anywhere in
the pipeline, so an `fdev`-specific packaging break (the #4240 class) is now
discovered at the very last and most expensive step: after the tag, ~30-60
minutes of cross-compilation and macOS notarization, and Gate A.

This is a deliberate, accepted cost, not an oversight — and it is worth being
explicit that it is the same "fail at the expensive moment" shape this pipeline
otherwise works to avoid. It is tolerated only because every alternative is
worse:

- `cargo publish -p fdev --dry-run` **cannot work** before `freenet` is
  published. `crates/fdev/Cargo.toml` carries
  `freenet = { path = "../core", version = "X.Y.Z" }`, so packaging `fdev`
  strips the path and the verification build resolves `freenet` from the
  registry — at a version that does not exist yet. It would fail every release
  for a reason that is not a bug.
- `--no-verify` would let it run, but skips the verification build, which is the
  step that actually catches the #4240 class. That is a check that cannot fail —
  the shape this repo keeps having to remove.
- A `[patch.crates-io]` override pointing `freenet` at the local path would make
  the dry run resolve. **`.claude/rules/git-workflow.md` explicitly forbids this
  pattern**, having been burned by it: patches are not inherited by nested
  workspaces, CI cannot resolve path deps on a fresh checkout, and it leaves a
  pre-merge cleanup step behind. Not worth it for a pre-flight.

The blast radius is bounded: `freenet` is already published by the time `fdev`
is attempted, so an `fdev` failure leaves a recoverable partial publish, and an
`attach-to-release` re-run skips `freenet` and retries `fdev` alone (see the
section below). If an `fdev` packaging break ever actually happens, the cheapest
fix is a CI job running `cargo package -p fdev --no-verify` on PRs touching
`crates/fdev/` — catching manifest and include-path errors, though not
compile-time ones.

### `Publish crates to crates.io` failed (in cross-compile.yml)

If it failed with "please provide a non-empty token" or similar, the
`CARGO_REGISTRY_TOKEN` secret is missing or invalid. Update the secret, then
re-run the `attach-to-release` job: the step skips any version already on
crates.io, so a re-run is safe and keeps the publish ahead of the undraft.

If a single crate failed mid-publish (e.g. `fdev` failed but `freenet`
succeeded), the same re-run handles it — `freenet` is skipped as already
published and `fdev` is retried. Publishing by hand is a last resort; see
`scripts/RELEASE_RECOVERY.md` Step 4, and un-draft only after confirming
Gate A passed.

### The crates.io publish is downstream of Gate A

This is deliberate and it is the fix for the 0.2.124 loss. The publish used to
run in `release.yml`, before the tag was even pushed, so the one step in a
release that can never be undone happened before the gate that decides whether
to ship at all. When Gate A blocked v0.2.124 its crates were already permanent
on crates.io, the release could only ever stay a draft, and 0.2.125 had to be
cut in its place.

Now a Gate A block costs a **tag**, which is deletable. Delete the tag and the
draft, fix, re-cut on the corrected commit — but check first whether **that
exact version** is already on crates.io, because if it is, the version is spent
and you cut the next patch instead:

```bash
# 200 = published, 404 = not published. ANY other code is UNKNOWN, not "no".
# The -A is load-bearing: crates.io answers 403 without a descriptive
# User-Agent, and because 403 has a JSON body, a `| jq -e '.version.num'` form
# exits 0 and prints "not published" for EVERY version -- silently turning this
# check into the exact hazard described below.
curl -sS -o /dev/null -w '%{http_code}\n' -A 'freenet-release-driver' \
  --max-time 30 --retry 3 --retry-all-errors \
  https://crates.io/api/v1/crates/freenet/X.Y.Z
```

**Not `cargo search`.** This is the one decision where it gives the wrong answer
in the dangerous direction: it reports only a crate's NEWEST version and reads
the search index, which lags the registry, so it can answer "not published"
about a version that IS published — and acting on that means re-tagging a spent
version, which is the unrecoverable 0.2.124 state this whole ordering exists to
prevent.

The rule, since `cargo search` is still correct in other places and should not
be purged: **`cargo search` may only answer questions about the NEWEST
version.** Asking "is the newest published version X?" is fine (that is what
`release.sh`'s version-comparison guard does). Asking "is version X published?"
is not.

`scripts/release_canary_wiring_test.sh` pins this ordering — the publish must
sit between the canary and the undraft, and `release.yml` must contain no
non-dry-run `cargo publish` — because moving it back would otherwise be
invisible to every test in the repo.

### `create_release` failed

Most likely cause: tag push failed because git identity wasn't configured on
the runner. Fixed by [PR #4135] — if it recurs, check that the
`Create and push tag` step still has `git config user.name` /
`git config user.email`.

To unblock manually:

Tag the **release commit**, not `main`. The release is cut from one pinned
commit — the version-bump PR's merge commit — and `main` may have moved past
it since (that is #5233; tagging `main` reintroduces exactly the bug the
pipeline now prevents). Resolve it the same way the workflow does:

```bash
# The bump PR is the "build: release X.Y.Z" one; the run log also prints
# "📌 Release pinned to <sha>".
RELEASE_SHA=$(gh pr view <BUMP_PR> --repo freenet/freenet-core \
  --json mergeCommit --jq '.mergeCommit.oid')

# Sanity-check it really is the bump commit before tagging it.
git show "$RELEASE_SHA:crates/core/Cargo.toml" | grep '^version'

git tag -a vX.Y.Z "$RELEASE_SHA" -m "Release vX.Y.Z"
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

The `Gateway Update` workflow polls `/version` for 120 s after the POST. It
now requires BOTH that the on-disk binary reports the new version AND that the
gateway service is actually `active` on it (the `service_active` field). So a
converge timeout now has two possible meanings:

- the binary never updated (download/swap failed), OR
- the binary swapped but the **service failed to restart** — this is the
  vega v0.2.71 case, where `/version` reported the new version while the
  gateway was down. Check `ssh <host> 'sudo systemctl status freenet-gateway'`
  (or `freenet-gateway-hector` on vega's secondary instance); a `failed`/
  `inactive` unit with the new binary on disk is this failure mode.

(Note: against a gateway running an OLD release-agent that predates the
`service_active` field, the workflow can no longer confirm the service is
running. As of #4492 it **fails closed** by default — the rollout step errors
at the deadline telling you to update that gateway's release-agent. The old
binary-only behaviour is available only via the explicit
`allow_binary_only_fallback=true` `workflow_dispatch` input, for a deliberate
rollout to a gateway you know runs an old agent. So on the normal
release-triggered path, a gateway with an outdated agent will fail the rollout
until its agent is upgraded.)

Recovery:

1. Check the agent's status: `ssh ian@<host> 'sudo systemctl status
   freenet-release-agent && sudo journalctl -u freenet-release-agent
   --since "10 min ago"'`
2. If the agent is fine but the service is down, restart it directly:
   `ssh ian@<host> 'sudo systemctl restart freenet-gateway'` (then confirm
   `systemctl is-active freenet-gateway`). If the script itself failed, the
   manual fix is `ssh ian@<host> 'sudo /usr/local/bin/gateway-auto-update.sh
   --force'`.
3. Re-run the gateway-update workflow against just the failed gateway:
   `gh workflow run gateway-update.yml --field version=X.Y.Z --field
   gateways=nova`.

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

## Auto-update canary (#5222)

Auto-update was broken fleet-wide for **two consecutive releases** (v0.2.120
and v0.2.121) without anything noticing. #5104 made the node's release-tag
fetch return the tag verbatim (`v0.2.121`) and normalised it at only one of its
two consumers; the detection path kept the raw tag, `semver` parsing failed,
and every update was dropped with a `warn!`. ~1,100 nodes had to be told to run
`freenet update` by hand, because a broken updater cannot deliver its own fix.

Every signal we had was one-sided — the release built, published, installed and
ran. Two gates now close that, both driven by `scripts/auto-update-canary.sh`:

**Gate A — pre-flight, BLOCKING** (`attach-to-release` job in
`cross-compile.yml`, between asset upload and the crates.io publish). Boots the
binary that is about to ship and requires its updater to read GitHub's current
release tag. Runs while the release is still a draft and before anything has
been uploaded to crates.io, so a failure costs a deletable tag rather than a
stranded fleet or a spent version number. Adds about a minute.

**Gate B — self-update, post-publish** (`auto-update-selfupdate-canary` job).
Takes the *previous* release and requires it to detect this one, exit 42, and
self-replace via `freenet update`. This is the transition the fleet actually
makes. It cannot run earlier: the detection path is hardwired to GitHub's
`/releases/latest`, and a draft release does not appear there. A failure is
loud (red job plus a River dev-room message) but does not block, since the
release is already public by then.

Both assertions are deliberately **two-sided**: the `Startup update check
against GitHub` line must be PRESENT *and* there must be no
`Startup update check: failed to parse` warning (the marker stops at
`parse` so it covers the current-version arm as well as the latest-version
one). Absence of the error on its own
proves nothing — it is equally consistent with the check never running, which
is exactly what `--disable-auto-update` or a dirty build produces. The canary
also fails if the node under test has auto-update disabled at all, so
"the canary was silently turned off" is a red build rather than something
someone has to remember. (It had been forgotten: `framework`, the designated
real-NAT pre-release smoke peer, ran with `--disable-auto-update` for nine days
after a #5040 measurement window, which is why it never caught this.)

### What the gates do NOT cover

Worth knowing before you conclude "we have an auto-update canary, why didn't it
catch this?"

**Gate A proves exactly one chain: fetch → tag normalise → semver parse →
compare.** That is the #5221 break and nothing more. It does *not* exercise
signature verification, checksum-manifest matching, asset download, the binary
swap, the exit-42 supervisor plumbing, or crash-loop rollback. A release whose
*detection* works and whose *installer* is broken passes Gate A cleanly.

**Gate B does cover download, signature, checksum and swap — but it runs the
PREVIOUS release's binary**, because a node can only self-update *from*
something. So a break in the installer half of the binary you are shipping is
caught by Gate B one release later, when that binary becomes the previous one.
Gate B is also post-publish and non-blocking, so even then it reports rather
than stops.

**Gate A checks the COMPARISON in one direction only.** Since #5236 it checks
the version the node says it observed (`latest=`) against the tag
`releases/latest` actually resolves to, so a fetch or normaliser that returns
the wrong string is caught. Since #5340 it also asserts WHICH decision the
updater reached, which closes the half of the comparison Gate A is able to
exercise.

The half it closes. Gate A's subject is by construction NEWER than
`releases/latest` — the release it belongs to is still a draft — so a healthy
updater must find nothing newer and stay put. Anything else is a defect, and
the obvious reading of the failure is the wrong way round: inverting
`compare_versions_for_startup`'s `latest_ver > current_ver`
(`crates/core/src/bin/commands/auto_update.rs`) does not make the node quietly
do nothing. `latest < current` is TRUE for a Gate A run, so the node returns the
OLDER release, requests an update to it, and exits 42 — a self-downgrade, and a
supervisor loop that repeats it on every restart. Gate A used to report green on
that, because a trigger was one of the outcomes `assert_detection_healthy`
accepts. It now blocks on it, from two independent observers: a trigger line
appearing in the log, and `NODE_EXIT` being 42. (A healthy run has neither.)

The two are independent in the way that matters, and the exit-code observer is
what covers the log check's blind spot rather than merely duplicating it. If a
trigger site's wording drifts out of `MARKER_TRIGGERED_RE`, or the line is
dropped, the node logs no matched trigger *and* no completion line either —
`freenet.rs` returns as soon as it sends — so the log assertion can only report
INDETERMINATE. The exit-code observer is therefore evaluated whenever the log
assertion has not already found a definite fault, not only when it passed;
gating it on "passed" made it unreachable on exactly that input, which an
external review pass caught and reproduced.

The second observer is qualified — 42 is also `FATAL_LISTENER_EXIT_CODE`
(`crates/core/src/node/p2p_impl.rs`), which a healthy binary emits when its
network event listener dies or redb is poisoned, and the canary does not opt
into the distinct code 45. So an exit 42 whose node output carries one of those
`CRITICAL:` lines is classified environmental and retried rather than blocking
the release. (A healthy Gate A run ends at 143 — the canary SIGTERMs a node that
is still going — so a 42 at all means the node exited on its own.)

Gate A also blocks a `is_version_pinned_bad` / `is_version_install_gated` gate
(`crates/core/src/bin/commands/rollback.rs`) that matches when it should not:
the canary node has a fresh isolated HOME with no crash-loop pin and no
install-failure history, so that refusal has nothing it could legitimately
match, and a node that refuses every release it is offered is stranded as
thoroughly as one that cannot parse the tag. That check is worth having and is
close to unreachable on the normal path, which is not a contradiction — the node
only reaches the #4073 branch from inside
`if let Some(new_version) = startup_update_check(…)`, so entering it during a
draft-release run already requires an inverted comparator, and the trigger check
above would catch that first. Where it earns its keep is the older-binary arm
and Gate B.

The half it does not close, and cannot. A comparator that answers "nothing
newer" when something newer DOES exist is invisible on the normal path, because
on the normal path nothing newer exists. Gate A only asserts that direction when
it is genuinely running an older binary, which it detects by comparing the
shipping binary's `--version` against the resolved latest tag; it says which arm
it took in the job log. That arm is narrow, and narrower than it first looks:
`cross-compile.yml` sparse-checks-out the canary script at the tag it is gating,
so re-running an *older* release's workflow runs that tag's copy of the script,
which predates this check entirely. What is left is a hotfix branch cut on an
older line and tagged after a newer release has published, a re-run of a tag cut
after #5340 landed, and a manual `preflight` run. The conditional exists to stop
a blocking gate from failing those for being right, not because they are common.

If the two versions cannot be compared at all, Gate A skips the decision check
and the exit-code check with it, and emits a `::warning::` annotation saying so.
That is the one input that returns Gate A to its pre-#5340 strength while still
reporting green, so the `--version` output format is source-pinned in
`auto-update-canary_test.sh` — a change to it fails there rather than widening
that arm quietly.

What does cover the comparison in both directions is the Rust unit tests on
`compare_versions_for_startup` (same file, `mod tests`), which assert both
directions and the equal case. Gate B covers it end-to-end for real, but only
for the PREVIOUS release's binary.

**An orphaned node was observed once, and the pin cannot see it.** After a real
`preflight` returned, a `timeout 240 target/release/freenet network …` was still
alive about four minutes later, its workdir already deleted by the EXIT trap. It
did not reproduce: 0 leaks in 9 subsequent runs, so there is no known rate and
no mechanism. Lifecycle case 4 pins exactly this property, but against a bash
fake node with none of a real node's SIGTERM handling or graceful shutdown, so
the environment that test runs in cannot produce the fault — a green case 4 is
not evidence the leak is gone. If a canary run ever seems to hang or a later run
reports "the startup update check never ran" for no clear reason, check for a
stray `freenet network` process first; a leaked node holds its ports and burns
CPU, which is how this surfaced before (see the lifecycle test's case 2 notes).

**Gate B's own code is never executed by any test.** The two gaps above are
about what the gates cannot observe when they run. This one is about the tests
*behind* the gates, and it is worth stating separately because it is easy to
mistake a large green suite for coverage it does not have.

`scripts/auto-update-canary_test.sh` runs the canary's pure helpers against
fixtures and source-scrapes the rest. `cmd_selfupdate` — the whole of Gate B —
is never invoked, and neither is `resolve_expected_latest`. So nothing at any
level runs the previous release's tarball download, the extraction check, the
exit-42 assertion, `freenet update --quiet`, or the `awk '{print $3}'` field
split that reads the updated binary's version. Those run for the first time
during a real release, against a real GitHub.

What the suite does pin around that code is real, and the distinction matters
when reading a failure: the version gate that decides whether Gate B's equality
check arms is tested behaviourally (`prev_emits_latest_seen`), and the call
site that consumes it is pinned including its `if ` prefix, so neither a
negation nor a reworded call can disarm the gate silently. That is the decision
logic. The I/O sequence it guards has no test.

Practical consequence: a Gate B failure is more likely to be the canary's own
plumbing than the fleet's updater, and it is non-blocking either way. Read the
job log before concluding anything about auto-update. Closing this needs a
runtime test with a stubbed release archive; it is a known gap, deliberately
deferred, not an oversight.

Net: the installer half of a shipping binary has no blocking gate, and its
comparison logic is gated in one direction only. Treat a green Gate A as "this
binary can still fetch and read new release tags, and did not ask to be
replaced by an older one", not as "auto-update works".

### If Gate A fails

The release stays an **unpublished draft** with all assets attached. That is
the correct state — do not un-draft it by hand to unblock the release. The
updater in that binary cannot read GitHub's release tags, so publishing it
strands every node on the previous version and the fix cannot be delivered
automatically. (`scripts/release.sh` will also refuse to publish while the
cross-compile run is unfinished or failed, for the same reason.)

**Know the state you are in first — and this section described the OLD ordering
until the publish moved.** `release.yml` no longer publishes to crates.io at
all; it only dry-runs. The real upload happens in `cross-compile.yml`'s
`attach-to-release`, **after** Gate A. So when Gate A blocks, the canary has run
and the publish step has not: **nothing was uploaded, and the version is not
spent.**

That is the whole point of the reorder. A Gate A block costs a **tag**, which is
deletable — see "The crates.io publish is downstream of Gate A" above, and
`scripts/RELEASE_RECOVERY.md` Step 4b.

Confirm it rather than assuming, because the recovery differs completely:

```bash
# 200 = published (version spent), 404 = not published (re-cuttable).
# The -A is required: crates.io answers 403 without a descriptive User-Agent,
# and a body-parsing form reads that 403 as "not published" for every version.
curl -sS -o /dev/null -w '%{http_code}\n' -A 'freenet-release-driver' \
  --max-time 30 --retry 3 --retry-all-errors \
  https://crates.io/api/v1/crates/freenet/X.Y.Z
```

- **404 — the normal case after a Gate A block.** Nothing irreversible has
  happened. Delete the tag and the draft, fix, and re-cut the SAME version on
  the corrected commit.
- **200 — the version really is spent.** Only reachable if the publish step ran
  and something later failed. Do not re-tag it; cut the next patch instead, and
  note that `cargo binstall freenet` will 404 and the nightly
  `binstall-smoke-test` will go red until a published release exists.

1. Read the job log. It distinguishes a genuine parse failure from `UNVERIFIED`
   (GitHub was unreachable) or a port collision (exit 43, another node or
   canary run on the host — re-run the job).

   It does not always name an offending *line*, and an earlier version of this
   step said it did. The parse-fail and fetch-fail branches echo the offending
   log lines; the two branches most likely to fire on a healthy release — "the
   check never ran" and "started but never logged an outcome" — have no single
   line to name. Those now print a `canary node evidence` group holding the tail
   of `node.out` and of the node log, because the canary deletes its workdir on
   exit and a blocking run used to leave nothing at all behind. Read that group
   first: a startup failure there (gateway-list fetch, config, port bind) means
   the node never reached the update task, and the update path is not the
   problem.
2. **If the failure was `UNVERIFIED` or a job timeout**, it is infrastructure,
   not a bug: use **Re-run failed jobs** on the cross-compile run. The build
   artifacts persist, so `attach-to-release` re-runs on its own and publishes
   if the canary passes.
3. **If the updater is genuinely broken**, fix the detection path
   (`crates/core/src/bin/commands/auto_update.rs`). Then, per the check above:

   - **crates absent (404, the normal case)** — delete the tag and the draft and
     re-cut the SAME version on the corrected commit. Nothing was published, so
     the version number is not spent, and burning one here would be conceding
     the loss this ordering exists to prevent.
   - **crates present (200)** — the version is spent; cut the next patch and
     leave vX.Y.Z's tag and draft in place.

   Publish the draft only if you have decided the broken updater is acceptable,
   knowing the fleet will not auto-update off it.
4. To reproduce locally, run the canary against a **clean release build**:

   ```bash
   bash scripts/auto-update-canary.sh preflight ./target/release/freenet
   ```

   Note that a build from a dirty working tree disables auto-update entirely
   (`build_info::GIT_DIRTY`), so the canary will report *auto-update is
   DISABLED* rather than reproducing the parse failure. Commit or stash first.

   A clean run here is **not** evidence that the harness is sound. CI stages
   the binary differently — `cross-compile.yml` puts it at `/tmp/freenet`,
   which used to collide with a directory the node created under `$TMPDIR` and
   blocked v0.2.124 on a healthy binary. That mkdir is gone (#5291), but the
   staging-environment difference it exposed is not, so the warning stands.
   Running from `./target/release/` is
   precisely the environment where that class of fault cannot occur, which is
   why local validation went 4/4 green while CI blocked. If local reproduces
   nothing, suspect the staging environment before the binary.

### If Gate B fails

**A red Gate B is not by itself a fleet problem, and the Matrix message is not
enough to tell.** Five distinct outcomes end in a red job; only one of them means
a node on the previous release genuinely cannot reach this one. **Read the
`::error::` line in the job log before doing anything** — it names which.

The wording below is generated from the code, so match on the quoted phrases
rather than on the shape of the alarm.

| What the log says | Exit | Matrix | What it means | Response |
|---|---|---|---|---|
| `UNVERIFIED (ENVIRONMENTAL): … could not reach GitHub … and this runner cannot reach it either` | 75 | ⚠️ quiet | The network was down for both the node and the runner. Nothing learned. | Re-run the job. |
| `UNVERIFIED (ENVIRONMENTAL): every attempt hit a port collision on this host` | 75 | ⚠️ quiet | Something else on the runner held the ports; the node never started. | Re-run the job. |
| `UNVERIFIED: … reported it could not reach GitHub … but THIS RUNNER reached the same endpoint immediately afterwards` | 1 | 🚨 loud | The published binary consistently could not do what the runner just did. Most likely its persisted poll-budget cooldown (#5102), possibly a published fetch-side regression. **Not a stranded fleet.** | Read the node output. Re-run; if it recurs across releases it is not the runner. |
| `UNVERIFIED: at least one attempt started the update check and never logged an outcome` | 1 | 🚨 loud | A hung updater, or the check was cut short. Genuinely unknown. | Re-run. Persisting, treat as a real fault. |
| Anything naming a specific detection or install failure | 1 | 🚨 loud | The real thing. See case 3. | See case 3. |

**The trap this table exists to remove.** An earlier version of this section said
a fetch failure always gives exit 75 and the quiet ⚠️, and told the reader that
if they were looking at the 🚨 text "this is not what happened". That stopped
being true when the corroboration probe landed: the GitHub cause now *also*
requires this runner to fail the same fetch, and a hosted runner is normally
online by probe time. So the common case — the previous release's binary logging

```
Startup update check: failed to fetch latest version: error sending request
for url (.../releases/latest). Continuing with current binary.
```

on an otherwise healthy runner — produces **exit 1 and the loud 🚨**, and the old
text sent the reader straight past it into "a real detection failure" and on to
cutting a fix release for a poll-budget cooldown.

**1. Environmental (exit 75, quiet ⚠️).** Two causes, both above. The previous
release's binary retries its startup fetch zero times, so a single bad moment on
the network is enough to produce the first. Gate B retries (`CANARY_ATTEMPTS`, 2
by default) and only reports this if every attempt lands the same way AND this
runner also cannot reach the endpoint. The job stays red: unverified is not
verified. Adding a retry to the node's own fetch
(`crates/core/src/bin/commands/auto_update.rs`) would shrink the class, but only
for releases published after that lands, since Gate B always drives an
already-published binary.

If either of these appears on **consecutive** releases, stop treating it as
noise: nothing has been verified since the last green run, and for the GitHub
cause a persistent node-side failure that this runner does not share is a
published fetch-side regression rather than weather.

**2. A stale canary constant, which is NOT a fleet problem either.** Gate B's
positive-equality check greps the previous release's log for
`MARKER_LATEST_SEEN` (`scripts/auto-update-canary.sh`), and it arms only from
`MARKER_LATEST_SEEN_SINCE` onwards. If that marker's TEXT was reworded and the
constant was not moved to the release that first ships the new wording, Gate B
demands text the published binary was never built to emit. Auto-update is fine;
the gate is asking the wrong question.

Check first:

```bash
grep -n "MARKER_LATEST_SEEN\b\|MARKER_LATEST_SEEN_SINCE" scripts/auto-update-canary.sh
```

If the marker text was changed in this release's window, fix the constant (the
test file freezes the two together and explains the choice) rather than shipping
anything. The same applies to the other markers Gate B greps against the
previous binary — `MARKER_DISABLED`, `MARKER_CHECK_RAN`, `MARKER_CHECK_COMPLETE`,
`MARKER_TRIGGERED_RE` and `MARKER_FETCH_FAIL` — which are not frozen and would
produce the same false alarm with a less specific message (#5309 tracks the
first four; it does not name `MARKER_FETCH_FAIL`).

`MARKER_FETCH_FAIL` is the one of those five to check first. It is no longer
just a marker Gate B greps: it is the input to the environmental classification
in case 1. Reword it and a previous release's failed fetch stops being
recognised as environmental at all, so even the genuinely-offline case fires the
loud 🚨 — reinstating the false fleet alarm that classification exists to
remove, from an edit that looks unrelated to it.

`MARKER_PARSE_FAIL` is the one to be most careful with, and it IS frozen
(`auto-update-canary_test.sh`), because it is the only marker whose reword fails
in the PASSING direction: it feeds Gate B's negative check, so a grep that stops
matching the text published binaries emit reports `OK: parsed GitHub's response`
for a release carrying the live #5221 bug.

**3. A real detection or install failure.** The previous release genuinely
cannot reach this one. The release is already public and the fleet will **not**
converge onto it on its own: ship a fix release, and expect to roll existing
nodes by hand (`freenet update`) as v0.2.120/v0.2.121 required.

Gate A does not have the case-2 failure mode: it runs a binary built from the
same tree as the canary, so a reword there is self-consistent. Gate B is the
only place an OLDER binary's log is read.

## Post-release verification

After the cascade completes, do these checks (or use the `freenet-release`
verification skill if you have it):

1. <https://crates.io/crates/freenet> shows the new version.
2. <https://github.com/freenet/freenet-core/releases/tag/vX.Y.Z> is
   published (not draft) with 14 assets.
3. `curl https://nova.locut.us/release-agent/version` returns the new version,
   and `systemctl is-active freenet-gateway freenet-gateway-2` on nova reports
   both units `active`. These are two separate checks: the first confirms the
   binary was swapped, the second that BOTH gateway processes came back up —
   the second gateway follows the first via `WantedBy=`, and a companion that
   fails to start is the case `verify_service_active` now catches.
4. Matrix room shows the announcement.
5. `sudo journalctl -u freenet-gateway --since "30 min ago"` on each
   gateway shows no errors.
6. The `Auto-update self-update canary` job in the tag's `cross-compile` run
   is green — a node on the previous release reached this one on its own. If
   it is red, read the job log before concluding anything: a red job can mean
   the fleet is stranded OR that the run was environmental (exit 75, quiet ⚠️
   Matrix message, re-run it). See "If Gate B fails" above.

[PR #4135]: https://github.com/freenet/freenet-core/pull/4135
[river#241]: https://github.com/freenet/river/issues/241
