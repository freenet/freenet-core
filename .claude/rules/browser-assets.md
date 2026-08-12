# Browser Assets (JS the node injects into pages)

Applies to `crates/core/src/server/path_handlers/assets/` — `shell_bridge.js`,
`navigation_interceptor.js`, `websocket_shim.js`, and anything else served into
a real browser — plus the HTML/CSP the node wraps around them
(`path_handlers.rs`, `client_api.rs`).

## Why this directory has its own rules

These files are `include_str!`-embedded into the node binary, so a node upgrade
changes the JS running in **every** locally-served web app at once — River,
Delta, Atlas and any third-party app — with no contract republish and no WASM
re-key. That is a nice property for shipping fixes and a dangerous one for
shipping bugs: the blast radius of a mistake here is every app on every
upgraded node simultaneously.

The recurring failure mode is that **the whole Rust test suite plus the
`riverctl` smoke tests can be green while every app in a browser is broken**.
Rust tests assert substrings of the emitted JS; they do not execute it, they
have no DOM, no sandboxed iframe, no CSP, no per-origin connection budget, and
no per-engine behaviour. `riverctl` is not a browser at all — it speaks the
contract API directly and never loads the shell.

Two production incidents, both green in CI:

- **#4945** (0.2.107) — reading `navigator.serviceWorker` throws a
  `SecurityError` in a sandboxed document without `allow-same-origin`. The
  property exists, so `'serviceWorker' in navigator` passed; the getter threw.
  An uncaught throw killed `freenetBridge()` before its message handlers
  installed, and every locally-served web app hung at loading. Only reproducible
  inside a genuinely sandboxed iframe.
- **#5213** — the shell subscribed to permission prompts with an `EventSource`,
  a normal HTTP request held open for the tab's whole life. Every Freenet app
  shares the node's single origin and browsers cap HTTP/1.1 at ~6 connections
  per origin, so six open tabs consumed the budget permanently and a seventh
  tab's document request queued forever with no error. Only reproducible with
  real tabs against a real connection scheduler.

The generalisable shape: **a browser-only resource budget or a browser-only
security boundary is invisible to every non-browser test.** Connections per
origin, sandbox/opaque-origin restrictions, CSP, popup blocking, storage
partitioning, and bfcache all behave differently — or only exist — in a real
engine.

## Rules

### 1. A behavioural change here needs a Playwright test, not only a Rust assertion

The suite lives in `crates/core/tests/playwright/`, is driven by
`crates/core/tests/playwright_shell.rs` (which boots a node via
`#[freenet_test]`, publishes a fixture webapp, and exports `FREENET_SHELL_URL`),
and runs in `.github/workflows/playwright-shell.yml`, path-filtered to
`crates/core/src/server/**`.

Know its limit: that workflow is **not a required check**, so the strongest
guard for these invariants cannot block a merge while the weaker substring pins
can. Tracked in #5275. Until that is fixed, treat a red Playwright run as
blocking by hand even though CI will not enforce it.

Keep the Rust substring assertions — they are a fast local signal — but do not
mistake them for the guard. State their scope in a comment where they live.

### 2. Pin the invariant, not the spelling of one violation

`!html.contains("EventSource(")` rules out exactly the construct that
shipped. A streamed `fetch()` or a long-poll breaks the same invariant and
passes. Where a real invariant exists ("the shell holds no HTTP request open"),
assert it behaviourally: see
`crates/core/tests/playwright/tests/connection-exhaustion.spec.ts`, which loads
the shell, waits, and fails if any request is still unfinished.

### 3. Test in all three engines, and make sure the assertion can fail there

`playwright.config.ts` runs chromium, firefox and webkit deliberately. Per-engine
divergence is the norm in this directory: #5087 (blank new tab) reproduces only
in WebKit, #5106 (dead click) only in Firefox, and the #3818 sandbox-escape
guard passes **vacuously** in Chromium, where the escape dies a step earlier and
the assertion runs against an empty set. A chromium-only matrix is how #5087
shipped green. Do not narrow the matrix.

### 4. Reproduce in the real context, not a convenient one

A top-level `eval` is not a sandboxed iframe (#4945 needed an opaque origin);
separate `BrowserContext`s do not share a connection pool (#5213 needed one
context); and a feature-detect that reads a property is not the same as one that
survives a throwing getter. If the test does not run where the bug lives, it
cannot fail. Ask what input would make the new assertion red, and confirm it.

### 5. Assume every open tab pays the cost

Anything the shell holds — a connection, a timer, a listener, an observer — is
held once per open tab, and users routinely keep several Freenet apps open
against one origin. Before adding a persistent resource, multiply it by ten
tabs and check the browser-imposed ceiling.
