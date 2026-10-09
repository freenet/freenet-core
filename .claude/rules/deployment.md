---
paths:
  - "crates/core/src/bin/**"
  # The build script is what DECIDES the auto-update gate below (it emits
  # GIT_DIRTY), and this rule read as complete while never loading on a change
  # to it.
  - "crates/core/build.rs"
  - "Cargo.toml"
  - "crates/*/Cargo.toml"
  - "apps/freenet-ping/**"
  - "*.service"
  # The Nix deployment path: package.nix supplies the build-time provenance that
  # feeds the same gate, and nix/freenet-node.sh is a supervisor bound by the
  # exit-code rule at the top of this file.
  - "package.nix"
  - "flake.nix"
  - "nix/**"
---

# Deployment Resilience Rules

## Trigger-Action Rules

### WHEN adding expected exit codes

```
Expected exit codes MUST be declared to the service manager.

Example (systemd):
  SuccessExitStatus=42

WHY: systemd counts unknown exit codes as failures. After N rapid restarts
(e.g., intentional "update needed" exit), systemd permanently stops the service.
```

### WHEN implementing auto-update

```
Auto-update MUST be disabled for dev/dirty builds.

CHECK: env!("VERGEN_GIT_DIRTY") or equivalent build metadata
  → If dirty/dev: Skip auto-update entirely
  → If release: Proceed with update

WHY: Dev builds triggering auto-update replaces the dev binary with a release
binary, destroying the development environment.

The GIT_DIRTY gate only covers a DIRTY tree. A CLEAN build that is not an
official release but intentionally runs AHEAD of the latest release (e.g. the
try.freenet.org from-source node) has GIT_DIRTY empty, so it is NOT gated — it
would detect the newer published release and exit 42 in a loop. For that case,
pass `--disable-auto-update` on that deployment's `ExecStart` (default is off,
so release nodes are unaffected). See #4690.

There are THREE ways the gate is controlled, not two. The third is BUILD-TIME
and is the easiest to set by accident:

  1. `GIT_DIRTY` non-empty      — a dirty working tree, detected by build.rs
  2. `--disable-auto-update`    — a runtime flag, deliberately with no `env`
                                  binding (config.rs explains why)
  3. `FREENET_GIT_IS_DIRTY=1`   — a BUILD-TIME env var read by
                                  crates/core/build.rs, which forces (1)

(3) exists so a packager with no `.git` (release tarball, `nix build`, distro
source drop) can state the provenance build.rs would otherwise probe for. It is
parsed STRICTLY — only `1`/`true`/`0`/`false`/empty, anything else fails the
build — precisely because it is the kill switch: a lenient truthiness rule would
read `FREENET_GIT_IS_DIRTY=false` as DIRTY and ship a release binary that
silently never updates itself. Empty means "no override", NOT "clean".

Its sibling `FREENET_GIT_COMMIT_HASH` has the same empty-means-no-override rule
and must be <= 40 hex characters. Passing a placeholder such as `unknown` FAILS
THE BUILD; pass empty instead and let build.rs's own fallback report `unknown`.
See docs/nix.md.
```

### WHEN tightening security (sandbox, CSP, CORS)

```
Security-tightening changes MUST be tested against the actual
capabilities that hosted apps require.

BEFORE adding sandbox attributes, CSP headers, or CORS restrictions:
  1. Inventory what hosted apps actually use (WebSocket, localStorage, fetch, etc.)
  2. Test the restriction against each capability
  3. Document which capabilities are allowed and why

WHY: iframe sandbox blocked CORS, CSP, and WebSocket that contract web apps
depended on. The security fix had to be fixed itself.
```

### WHEN adding or modifying a platform-gated code path

```
A `#[cfg(target_os = "...")]` branch that CI does not build and run on
that target is unverified, even if it compiles on other platforms.

BEFORE claiming the cfg'd path works:
  1. Smoke-test it on the actual target OS (boot the binary, exercise
     the code path, observe the expected behaviour) — compilation is
     not verification.
  2. Prefer extracting the platform-independent decision logic into a
     pure function that compiles on all targets so it can be unit-
     tested from the CI platforms that do exist.
     See: `dispatch_menu_event`, `compute_menu_state`, and
     `first_run_marker_*` in `crates/core/src/bin/commands/` for the
     pattern — behaviour split into a pure core + platform binding.
  3. If a comment asserts third-party library behaviour (e.g.,
     "the crate drives the NSRunLoop internally"), cite the source:
     upstream docs, an official example, or a verified smoke-test.
     An uncited assertion is a hypothesis, not a guarantee.

WHY: PR #3928 fixed a cfg'd `target_os = "macos"` tray/menu-bar path
that had shipped in the tree for months because no CI runner
exercised it. An uncited "tray-icon drives the NSRunLoop internally"
comment encoded the original mistake directly into the code.
```

### WHEN managing dependencies

```
Unused dependencies MUST be removed — they are latent build hazards.

CHECK periodically: cargo machete (or equivalent)
  → Remove any crate not actually imported or used
  → Pay special attention to platform-specific crates (e.g., wmi on non-Windows)

WHY: Unused crates can pull conflicting transitive dependency versions,
breaking cross-compilation even though the crate is never used.
```

## Checklist for Deployment Changes

```
□ Are all expected exit codes declared in service config?
□ Is auto-update gated on release builds?
□ Are security restrictions tested against real app capabilities?
□ Have unused dependencies been checked with cargo machete?
□ Does the change affect cross-compilation? Test on target platforms.
```
