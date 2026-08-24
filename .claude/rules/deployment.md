---
paths:
  - "crates/core/src/bin/**"
  - "Cargo.toml"
  - "crates/*/Cargo.toml"
  - "apps/freenet-ping/**"
  - "*.service"
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

### WHEN classifying a supervisor's stop reason

```
systemd's $EXIT_STATUS is NOT always a number. It is the numeric exit
code only when $EXIT_CODE is "exited"; for every other disposition it is
a SIGNAL NAME ("TERM", "SEGV", "ABRT", ...). A `case` (or a match) whose
arms are all numeric therefore falls through to its catch-all on every
signal death, silently scoring it as whatever the catch-all means.

BEFORE writing or changing such a classifier (ExecStopPost hooks,
`rollback.rs::classify_stop`):
  1. Decide explicitly what each of the THREE variables contributes:
     $SERVICE_RESULT (systemd's verdict), $EXIT_CODE (exited / killed /
     dumped), $EXIT_STATUS (number OR signal name). One of them alone is
     almost always the wrong key — see the table in `man systemd.exec`.
  2. Test by RUNNING the extracted script under a real shell across the
     whole result table, not by string-matching the unit template.
  3. Assert BOTH directions. A hook that both self-heals and COUNTS
     crashes can be disarmed by a too-broad skip, and a pin that only
     checks the skip stays green through that.

WHY: #5227 — a `case "$EXIT_STATUS" in 0|43)` had no arm for "TERM", so a
deliberate `systemctl stop` landing before the node installed its SIGTERM
handler fell to `*)` and was counted as a post-update probation crash.
Three of those roll a freshly-updated node back. Fixed in #5242; the
decision matrix lives in
`service::linux::tests::exec_stop_post_counts_crashes_and_skips_deliberate_stops`.
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
