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

### WHEN implementing auto-update

```
Auto-update MUST be disabled for dev/dirty builds.

CHECK: env!("VERGEN_GIT_DIRTY") or equivalent build metadata
  → If dirty/dev: Skip auto-update entirely
  → If release: Proceed with update

WHY: Dev builds triggering auto-update replaces the dev binary with a release
binary, destroying the development environment.
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
