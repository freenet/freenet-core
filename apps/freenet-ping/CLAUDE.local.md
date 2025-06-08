# Freenet Ping App - Important Notes

## Workspace Structure

This app has its own Cargo workspace defined in `Cargo.toml`. This is important because:

1. The app defines its own workspace dependencies that are inherited by its members (contracts/ping, app, types)
2. When using `workspace = true` in member crates, they refer to THIS workspace, not the parent workspace
3. Dependencies like `freenet-stdlib` must be explicitly versioned in this workspace's `[workspace.dependencies]` section

## Building with fdev

When `fdev build` runs for contracts, it builds them in isolation. This means:
- Workspace dependencies must be properly defined in the app's workspace
- The app workspace cannot use `workspace = true` to inherit from the parent workspace
- All dependencies must be explicitly versioned or use path dependencies

## Current Dependencies

- `freenet-stdlib = { version = "0.1.6", features = ["contract"] }` - Defined in app workspace
- Members use `freenet-stdlib = { workspace = true }` to inherit from app workspace