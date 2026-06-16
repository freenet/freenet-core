# Shell Playwright smoke tests

Browser smoke tests for the Freenet gateway **shell + sandboxed iframe
postMessage contract** (freenet/freenet-core#3856).

These exercise, against a real headless Chromium, the JavaScript the node
injects into the shell page and the sandboxed iframe — `SHELL_BRIDGE_JS`,
`WEBSOCKET_SHIM_JS`, `NAVIGATION_INTERCEPTOR_JS` in
`crates/core/src/server/path_handlers.rs` — and the CSP headers it serves
(`SHELL_PAGE_CSP`, `sandbox_csp_for_origin` in
`crates/core/src/server/client_api.rs`). Before this suite, that code was only
guarded by Rust-level substring assertions on the emitted JS; four regressions
(#3842, #3852, #3853, #3854) shipped to production through that gap.

## How it runs

The Rust harness `crates/core/tests/playwright_shell.rs` owns the node
lifecycle. It:

1. boots a single in-process gateway node via `#[freenet_test]`,
2. publishes `fixture-webapp/` as a Freenet **website contract** using the real
   `fdev website publish` path (with an isolated `XDG_CONFIG_HOME` so it never
   touches your `~/.config/freenet`),
3. waits until the shell route serves over HTTP, then
4. runs this Playwright project with the shell URL in `FREENET_SHELL_URL`.

The Playwright step is opt-in: the harness only launches the browser when
`FREENET_PLAYWRIGHT=1`. A plain `cargo test`/`cargo nextest` run publishes the
fixture, confirms the shell serves, and returns early — so a checkout without
Node/browsers never fails. The dedicated `.github/workflows/playwright-shell.yml`
job installs the browsers and sets the flag.

### Fixture WASM

The fixture is published with the **prebuilt** website-container WASM embedded
in `fdev` (`crates/fdev/resources/website_contract.wasm`). That keeps the test
free of a `wasm32-unknown-unknown` build step and makes the contract key
deterministic per fdev version. See `crates/website-contract/README.md` for the
(rare) procedure to rebuild that committed binary.

## Running locally

```bash
# one-time, from this directory:
npm ci
npx playwright install --with-deps chromium

# from the workspace root:
cargo build --bin fdev
FREENET_PLAYWRIGHT=1 cargo nextest run -p freenet \
  --features testing --test playwright_shell --no-capture
```

To iterate on the specs alone against an already-running node, point
`FREENET_SHELL_URL` at its shell route and run `npx playwright test` directly.

> Linux-only: the harness binds the node to a varied `127.x.y.1` loopback for
> test isolation. The full `127.0.0.0/8` range is loopback on Linux but not on
> macOS, where only `127.0.0.1` is reachable by default.
