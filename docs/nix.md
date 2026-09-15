# Freenet on Nix

Nix is a **supported deployment path** for Freenet, not just a way to get a
compiler. That has one hard consequence, and the rest of this page follows from
it:

> **A peer that does not auto-update is a liability for the network.**

Freenet ships security and protocol changes frequently, and a peer stuck on an
old release is a peer other nodes have to work around. So the flake's default
output is a node that keeps itself current.

## The two outputs

| Output | Auto-updates | Use it for |
|---|---|---|
| `packages.freenet-node` (also `packages.default`) | **Yes** | Running a peer on the Freenet network. |
| `packages.freenet` | No | Development, CI, and deployments where you want `nixos-rebuild` to decide which version runs. |

```bash
nix run github:freenet/freenet-core            # the supervised, self-updating node
nix run github:freenet/freenet-core -- --config-dir /srv/freenet   # arguments are forwarded
nix build github:freenet/freenet-core#freenet  # just the binary, result/bin/freenet
nix develop                                    # dev shell: pinned toolchain, nextest, shellcheck, ...
```

`freenet-autoupdate` is kept as an alias for `freenet-node` so anything wired up
against the older name keeps working.

## Why the self-updating node does not run from `/nix/store`

`freenet update` replaces the **running** binary in place: it resolves
`std::env::current_exe()` and renames a freshly-downloaded file over it
(`replace_binary`, `crates/core/src/bin/commands/update.rs`), after verifying
`SHA256SUMS.txt` against the ed25519 `FREENET_RELEASE_PUBKEY` baked into the
binary. `/nix/store` is read-only, so that updater cannot run from a store path.

So `freenet-node` does not try to make Nix the updater. It **seeds** the
nix-built binary into a mutable state directory once:

```
$STATE_DIRECTORY/bin/freenet                       # when systemd sets StateDirectory=
${XDG_STATE_HOME:-$HOME/.local/state}/freenet/bin/freenet   # otherwise
```

and from then on the node owns that file and updates it exactly as it does on
every other platform — same signature verification, same rollback snapshot, same
crash probation, same known-bad pinning. The wrapper never overwrites an
existing binary.

The supervisor itself (`nix/freenet-node.sh`) is a faithful port of the systemd
unit the node generates for itself (`generate_user_service_file`,
`crates/core/src/bin/commands/service/linux.rs`), and it makes **no update
decisions of its own**: it restarts the node and runs `freenet update`, and the
updater decides everything else. That is deliberate — an update path reimplemented
in shell would not be signature-verified, would not honour the known-bad pin, and
would not roll back a release that crash-loops.

The rejected alternative was `nix run github:freenet/freenet-core/$tag#freenet`
per release. It rebuilds from source on every release (there is no binary cache,
so the node is down for a full dependency-graph compile), bypasses the release
signature check, ignores known-bad pinning, and has no rollback.

### The tradeoff, stated plainly

**The running binary WILL diverge from the store path over time**, because it
updates itself. `nix path-info` and `systemctl show` will name the version this
generation was built from; `freenet --version` on the state-dir binary will name
whatever the node has updated itself to. That is not a bug to be fixed later — it
is the price of a peer that stays current, and it is why the two outputs exist.

If that divergence is unacceptable for your deployment — because you reproduce
hosts from a pinned flake, or because an auditor needs the running artifact to be
the one the closure describes — use `packages.freenet` and pass
`--disable-auto-update` on the node's command line. That flag exists for exactly
this case (#4690): a clean build that deliberately runs a version other than the
latest release would otherwise detect the newer release, exit 42 to request an
update, and be restarted onto the same version indefinitely. You then own keeping
the pin current, and the network is relying on you to do it.

To go back to a self-updating node after seeding one by hand, delete the
state-dir binary and let `freenet-node` re-seed it.

### Seed from a release, not from an arbitrary commit

`nix run github:freenet/freenet-core` follows the default branch, whose version
can be AHEAD of the latest published release. A node seeded from such a commit is
newer than anything GitHub publishes, so it correctly declines to update and sits
there until a release catches up — and if you seeded it from a commit that is
*behind*, it will simply update itself on first run and the seed is wasted work.
For a peer you intend to leave running, seed from a release tag:

```bash
nix run github:freenet/freenet-core/v0.2.135
```

After the first update the tag stops mattering: the state-dir binary is a real
release, and the node tracks releases from then on.

## Running it under systemd on NixOS

`freenet-node` is the process to supervise. It already handles the node's exit
codes internally, so the surrounding unit should be plain:

```nix
systemd.services.freenet = {
  wantedBy = [ "multi-user.target" ];
  after = [ "network-online.target" ];
  wants = [ "network-online.target" ];
  serviceConfig = {
    ExecStart = "${freenet-node}/bin/freenet-node";
    DynamicUser = true;
    StateDirectory = "freenet";   # the wrapper seeds the binary under this
    Restart = "on-failure";
    RestartSec = 30;
  };
};
```

Do **not** add `SuccessExitStatus=42 43` or `RestartPreventExitStatus=43` here:
those belong to a unit supervising `freenet network` directly, and
`freenet-node` already absorbs those codes. A read-only `/nix/store` is fine;
what must be writable is the state directory.

## What this does NOT give you

**It is not a way to verify the published release binaries.** Those are
statically-linked **musl** cross-builds produced by
`.github/workflows/cross-compile.yml`; a `nix build` here produces a
glibc-dynamic binary from a different toolchain configuration, and nothing
compares the two. Same version string, materially different artifact — which
also matters for release telemetry, since that keys on the version. A nix build
of tag `vX.Y.Z` reproducing bit-for-bit would say nothing about the binary
GitHub serves for that tag.

**Bit-for-bit reproducibility is not claimed and not gated.** The build is
*pinned and declarative* — same `flake.lock` and same source give the same
toolchain and the same dependency set, and `SOURCE_DATE_EPOCH` plus the git
provenance below remove the two obvious sources of drift. Whether two builds are
byte-identical has not been measured, so the docs do not assert it and CI does
not check it. Claiming it would need `nix build .#freenet --rebuild` in
`.github/workflows/nix.yml`, which doubles an already long compile; if someone
wants the property, add the gate in the same change that adds the sentence.

## Building outside Nix, without a repository

A packager working from a source tarball can supply the provenance that
`crates/core/build.rs` would otherwise read from `git`:

| Variable | Value | Effect |
|---|---|---|
| `FREENET_GIT_COMMIT_HASH` | up to 40 hex characters, or **empty** | Reported by `freenet --version`. Empty means "no override": build.rs probes `git`, and reports `unknown` if there is none. A non-hex value **fails the build** — never pass a placeholder like `unknown`. |
| `FREENET_GIT_IS_DIRTY` | `1`/`true`, `0`/`false`, or empty | Empty means "no override" (probe `git`). Anything else fails the build. |
| `SOURCE_DATE_EPOCH` | seconds since the epoch | Fixes `BUILD_TIMESTAMP`. |

`FREENET_GIT_IS_DIRTY` is **the third way to disable auto-update**, alongside a
genuinely dirty tree and `--disable-auto-update` — a build marked dirty never
updates itself. It is parsed strictly for that reason: a lenient "any non-empty
value is truthy" rule would read `FREENET_GIT_IS_DIRTY=false` as dirty and ship a
binary that silently never updates.

## Working in `nix develop`

The devShell's `shellHook` runs `unset SOURCE_DATE_EPOCH`. This is load-bearing,
not tidiness: nixpkgs' stdenv exports `SOURCE_DATE_EPOCH=315532800`
(1980-01-01), and `build.rs` honours it, so without the unset every `cargo build`
in the shell would stamp `Build timestamp: 1980-01-01T00:00:00Z` into the binary
— the field ops uses to correlate a running node's log with the artifact it came
from.

The shell's toolchain comes from [`rust-toolchain.toml`](../rust-toolchain.toml)
via `rust-overlay`, not from nixpkgs, so `cargo fmt` and
`cargo clippy -- -D warnings` inside it agree with CI.

## CI

`.github/workflows/nix.yml` builds both outputs and checks the devShell. It is
path-filtered to the nix files and is **not a required check** — see the note in
the workflow for why, and for who is expected to watch it.

`scripts/nix-node-wrapper_test.sh` drives `nix/freenet-node.sh` against a fake
`freenet` and asserts the exit-code contract in both directions (update runs on
42 and on a crash; does **not** run on 0, 43, or a signal-shaped status). It runs
in the main CI job, needs no Nix, and takes about a second.
