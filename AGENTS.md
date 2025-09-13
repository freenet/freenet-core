# Repository Guidelines

## Project Structure & Modules
- `crates/core` (crate: `freenet`): core node and the `freenet` binary.
- `crates/fdev` (crate: `fdev`): developer CLI for packaging, running, and tooling.
- `apps/*`: example apps and contracts (e.g., `apps/freenet-ping`).
- `tests/*`: integration test crates and app/contract fixtures.
- `scripts/`: local network helpers, deployment, and setup guides.
- `.github/workflows`: CI for build, test, clippy, and fmt.

## Build, Test, and Dev
- Init submodules (required): `git submodule update --init --recursive`.
- Build all: `cargo build --workspace --locked`.
- Run core: `cargo run -p freenet --bin freenet`.
- Install binaries: `cargo install --path crates/core` and `cargo install --path crates/fdev`.
- Test (workspace): `cargo test --workspace --no-default-features --features trace,websocket,redb`.
- Example app build: `make -C apps/freenet-ping -f run-ping.mk build`.
- Optional target for contracts: `rustup target add wasm32-unknown-unknown`.

## Coding Style & Naming
- Rust 2021, toolchain ≥ 1.80.
- Format: `cargo fmt` (CI enforces `cargo fmt -- --check`).
- Lint: `cargo clippy -- -D warnings` (no warnings in PRs).
- Naming: crates/modules `snake_case`; types/enums `PascalCase`; constants `SCREAMING_SNAKE_CASE`.
- Keep features explicit (e.g., `--no-default-features --features trace,websocket,redb`).

## Testing Guidelines
- Unit tests in-module with `#[cfg(test)]`; integration tests under `tests/*` crates.
- Prefer meaningful coverage for changed public behavior and error paths.
- Deterministic tests only; avoid external network unless mocked.
- Run: `cargo test --workspace` before pushing.

## Commits & Pull Requests
- Commit style: conventional prefixes (`feat:`, `fix:`, `chore:`, `refactor:`, `docs:`, `release:`). Example: `fix: prevent node crash on channel close`.
- PRs should include: clear description, rationale, linked issues, and test notes; attach logs or screenshots for app/UI changes.
- CI must pass: build, tests, `clippy`, and `fmt`.

## Security & Configuration
- Config and secrets use platform app dirs (via `directories`). Default config/secrets are created on first run; avoid committing them.
- Review changes touching networking, keys, or persistence; prefer least-privilege defaults and explicit feature gates.
