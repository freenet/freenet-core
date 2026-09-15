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
| `packages.freenet-node` (also `packages.default`) | **Yes** | Running a peer on the Freenet network. The only supported way to do that. |
| `packages.freenet` | No | **Building and development only** — the bare compiler output, for CI, `nix develop`, and packaging. |

**`packages.freenet` is not a supported way to run a peer.** It is just the
binary: nothing seeds it into a writable location and nothing restarts it, so a
peer started from it never updates itself. It will fall behind — Freenet ships
several releases on a busy day — and a peer far enough behind first becomes a
drag on the network and then stops working against it. If you are running a
peer, run `packages.freenet-node`.

```bash
nix run github:freenet/freenet-core            # the supervised, self-updating node
nix run github:freenet/freenet-core -- --config-dir /srv/freenet   # arguments are forwarded
nix build github:freenet/freenet-core#freenet  # build only: the bare binary at result/bin/freenet
nix develop                                    # dev shell: pinned toolchain, nextest, shellcheck, ...
```

`freenet-autoupdate` is kept as an alias for `freenet-node` so anything wired up
against the older name keeps working.

**If you apply `overlays.default`, note that `pkgs.freenet` is the bare binary.**
`environment.systemPackages = [ pkgs.freenet ];` puts a `freenet` on every
user's PATH that carries no supervisor and never updates itself, and nothing
about it says so. Use `pkgs.freenet-node` for anything that runs a peer.

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
crash probation, same known-bad pinning.

The wrapper asks exactly one question about that binary on every start: **can it
update itself?** If it can, the wrapper leaves it completely alone, whatever
version the store holds — which is what stops a peer being pinned back to the
flake's version. If it cannot, the wrapper replaces it with the store's copy.

The invariant is *always end up on a binary that can update itself*, and
deliberately **not** "never move backwards in version". A clean older binary is
forward progress: it exits 42 on its next start and walks itself to the current
release. A `-dirty` newer one is a dead end, because `GIT_DIRTY` is one of the
three auto-update kill switches, so the node never exits 42 again and nothing
moves it forward. Version ordering only separates two binaries that can *both*
update themselves, and there it decides nothing worth deciding.

"Cannot update itself" covers a binary that is absent, a symlink (which the
in-place updater renames *over*, replacing the link rather than the file it
names), a partially-written copy left by an interrupted seed, something that is
not a freenet binary at all, and a `-dirty` build. The first four cannot run; the
last one runs but is frozen. That difference decides how much the wrapper will
do:

* **It cannot run.** Anything runnable is better than nothing, so the store
  binary is installed even if it is itself `-dirty` or pinned known-bad — with a
  warning saying exactly what was installed.
* **It runs but is frozen.** The store binary replaces it only if the store
  binary can itself update. Trading one frozen binary for another gains nothing,
  and the one refusal that is genuinely about *which version* applies here: a
  version **this node has pinned known-bad**, after it crash-looped here and was
  rolled back, is not installed over a peer that is still serving. The wrapper
  installs binaries directly, with no known-good snapshot and no probation
  marker, so crash-loop rollback (#4073) could not fire the second time.

Every refusal says so on stderr rather than doing nothing quietly, and **a peer
whose binary cannot update itself is warned about on every node start**, not once
when it was seeded. A *first* seed from a dirty build is still allowed — there is
no working peer to protect — but it does not print the sentence promising that
the node owns the binary and will update it in place, because that promise would
be false.

One honest residual: the wrapper cannot see the node's auto-update failure
lockout, and re-seeding would not clear it if it could. The lockout is a counter
file in the node's own state directory, not a property of the binary. It does not
gate the wrapper's own `freenet update` (which clears the counter on a successful
install), so it only stops the node's in-process update re-poll — which means a
locked-out peer that never crashes also never updates. Closing that would mean
making update decisions in shell, which this wrapper deliberately does not do.

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

That divergence is the deal, and **there is no supported configuration in which
a peer stays pinned to its store path.** If you need the running artifact to be
exactly the one your closure describes — because you reproduce hosts from a
pinned flake, or an auditor needs the closure to describe what runs — then what
you need is not a pinned peer, it is to not run a peer on that host. A pinned
peer silently falls behind every release until it stops working, which is a cost
paid by the whole network rather than by whoever pinned it.

`--disable-auto-update` is **a development flag, not a deployment option.** It
exists (#4690) for a node deliberately running a build that is AHEAD of the
latest release, such as a from-source test node: without it that node detects
the newer published release, exits 42 to request an update, and is restarted
onto the same version indefinitely. Do not reach for it to hold a peer on a
pinned version; this page deliberately does not describe a way to do that.

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
# A real release tag at or after the first one containing this flake.
nix run github:freenet/freenet-core/vX.Y.Z
```

Check the tag actually has it (`nix flake show github:freenet/freenet-core/vX.Y.Z`
should list `freenet-node`). An OLDER tag has no `freenet-node` output at all,
and the `freenet-autoupdate` output it replaced was a different implementation
with no release-signature check and no rollback — so following a tag from before
this landed silently gets you a worse updater, not an older copy of this one.

After the first update the tag stops mattering: the state-dir binary is a real
release, and the node tracks releases from then on.

## Running it under systemd on NixOS

`freenet-node` is the process to supervise. It already handles the node's exit
codes internally, so the surrounding unit should be plain:

```nix
systemd.services.freenet-node = {
  wantedBy = [ "multi-user.target" ];
  after = [ "network-online.target" ];
  wants = [ "network-online.target" ];
  serviceConfig = {
    ExecStart = ''
      ${freenet-node}/bin/freenet-node \
        --config-dir /var/lib/freenet/config \
        --data-dir   /var/lib/freenet/data \
        --log-dir    /var/lib/freenet/logs
    '';
    User = "freenet";
    Group = "freenet";
    # /var/lib/freenet. The wrapper seeds the binary under $STATE_DIRECTORY/bin.
    StateDirectory = "freenet";
    # Load-bearing, not a default: the wrapper exits 0 for a stood-down peer
    # as well as for a clean shutdown — notably on exit 43, "another instance
    # already holds the port", where the holder may be a stale orphan (see the
    # #3967 KNOWN DIVERGENCE in nix/freenet-node.sh). Restarting only on
    # failure leaves such a peer dead forever with nothing to revive it. A real
    # `systemctl stop` still stops, because systemd knows it issued the stop.
    Restart = "always";
    RestartSec = 30;
  };
  # Also load-bearing, and NOT in `serviceConfig` — systemd's start limit lives
  # in `[Unit]`. Its default is burst 5 within 10s, which `RestartSec = 30`
  # never trips, so it looks like it does not matter. Lower `RestartSec` below
  # about 2s and the fifth exit puts the unit permanently in `failed` — the
  # #3967 "dead forever with nothing to revive it" outcome `Restart = "always"`
  # is here to prevent. `freenet-node` runs its own crash-loop limiter
  # (5 failures in 120s, then exit 1), so let that be the only one.
  startLimitIntervalSec = 0;
};

# Load-bearing, and the part that is easy to leave out. `StateDirectory` is NOT
# the only writable directory this needs: the node's auto-update state —
# the crash-probation marker, the known-good rollback snapshot and the
# known-bad version pin — lives under the service user's HOME
# (`auto_update::state_dir()` is `dirs::home_dir()/.local/state/freenet`), NOT
# under $STATE_DIRECTORY. A NixOS user declared without `home` gets
# `/var/empty`, which is not writable, so `prepare_known_good_for_install` and
# `begin_probation` both fail and the peer runs with #4073 crash-loop rollback
# silently OFF — a release that boot-crashes then has nothing to roll it back.
users.users.freenet = {
  isSystemUser = true;
  group = "freenet";
  home = "/var/lib/freenet";
  createHome = true;
};
users.groups.freenet = { };
```

The directories are named explicitly because the node otherwise derives them
from the service user's home, which a system user may not usefully have. A
read-only `/nix/store` is fine; **two** things must be writable, and they are
different directories: the state directory (`$STATE_DIRECTORY`, where the
wrapper seeds the binary) and the service user's home (where the node keeps its
auto-update rollback state). This wrapper already has to know they differ — it
looks for the known-bad pin in both — so an operator does too.

Do **not** add `SuccessExitStatus=42 43` or `RestartPreventExitStatus=43` here:
those belong to a unit supervising `freenet network` directly, and
`freenet-node` already absorbs those codes — it exits 0 for both.

For a *crash* loop it exits 1 once more than five counted failures land inside
two minutes, which is the case `Restart` is there to back-stop. That limiter deliberately does not cover
every loop: **exit 42 is burst-exempt**, because an update chain must not take a
peer offline, and 42 is also the node's fatal-listener code. So a node wedged at
boot — a port it cannot bind, say — exits 42 on every start, is never counted,
and instead of stopping it slow-flaps at the wrapper's 300s backoff cap
indefinitely. (`FREENET_SYSTEMD_FAST_CRASH` would make the node emit the
distinct fast-crash code 45 instead, and the wrapper deliberately does not set
it — see the header of `nix/freenet-node.sh`.) A peer restarting every five
minutes and never coming up is quiet in the journal; watch for it rather than
expecting the unit to fail.

**Do not name the unit `freenet`.** `freenet update` probes
`/etc/systemd/system/freenet.service` and `~/.config/systemd/user/freenet.service`
and rewrites the unit when it has drifted from the template the node generates
for itself (`ensure_service_file_updated`, `crates/core/src/bin/commands/update.rs`).
On NixOS that path is a symlink into the read-only store holding a unit Nix
owns, so every update would try — and fail — to rewrite it. Any name other than
`freenet` avoids the probe entirely.

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
| `SOURCE_DATE_EPOCH` | seconds since the epoch, or **empty** | Fixes `BUILD_TIMESTAMP`. Empty means "no override": build.rs uses the current time. A non-numeric value **fails the build**. |

`FREENET_GIT_IS_DIRTY` is **the third way to disable auto-update**, alongside a
genuinely dirty tree and `--disable-auto-update` — a build marked dirty never
updates itself. It is parsed strictly for that reason: a lenient "any non-empty
value is truthy" rule would read `FREENET_GIT_IS_DIRTY=false` as dirty and ship a
binary that silently never updates.

### `nix build 'path:.'` reports a CLEAN build from a dirty tree

Worth knowing before you read a version string as evidence. The flake derives
dirtiness from nix's own view of the source: `gitDirty = self ? dirtyRev`, which
is set only for a `git+file:` source that nix has determined to be dirty. A
`path:` source carries no VCS metadata at all, so neither `rev` nor `dirtyRev`
exists, `FREENET_GIT_IS_DIRTY=0` is passed, and that **suppresses build.rs's own
`git` probe** — so a build from a tree with uncommitted changes reports no
`-dirty` marker.

That is correct for the case the flag exists for (a tarball or unpacked source
drop, which genuinely has no VCS to probe and must not be guessed dirty — see
`gitDirty`'s comment in `flake.nix`), and it is what `.github/workflows/nix.yml`
depends on, because `nix build 'path:.'` is exactly how it covers the no-VCS
path that `actions/checkout` can never produce. But it surprises anyone using
`path:.` locally to test an uncommitted change: `freenet --version` will not say
`-dirty`, and the resulting binary **will** auto-update. Use `nix build .` (a
`git+file:` source) if you want nix's dirty detection, or pass
`FREENET_GIT_IS_DIRTY=1` explicitly.

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
`freenet` and asserts, by execution:

* the exit-code contract in both directions — `freenet update` runs on 42, on a
  crash, and on a signal aimed at the node alone; it does **not** run on 0 or 43;
* **which binary each invocation ran.** The fake logs `$0`, and both
  `freenet network` and `freenet update` must come from the state directory.
  Run either from the store seed and `current_exe()` is read-only, every update
  fails with EROFS, and after three failures the node stops asking to be
  updated at all — permanent silent staleness, and the cheapest possible
  regression to introduce;
* that the seed is a regular file, byte-identical to the store binary, and
  written atomically, so an interrupted first copy cannot brick the peer;
* the re-seed rules (strictly-newer only, never backwards, never on an
  unparseable version, and never onto a `-dirty` or known-bad-pinned store
  binary — both of which are newer and would leave the peer worse off);
* the growing, capped, jittered restart backoff, the bounded updater, and that
  a SIGTERM to the wrapper is honoured mid-pause.

It runs in the main CI job, needs no Nix, and takes about five seconds.
