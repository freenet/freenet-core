import { test, expect, type Page, type ConsoleMessage } from "@playwright/test";

// Smoke tests for the Freenet gateway shell + sandboxed iframe postMessage
// contract (freenet/freenet-core#3856).
//
// These exercise, against a real headless Chromium, the JavaScript the node
// injects into the shell page and the sandboxed iframe
// (crates/core/src/server/path_handlers.rs: SHELL_BRIDGE_JS,
// WEBSOCKET_SHIM_JS, NAVIGATION_INTERCEPTOR_JS) and the CSP headers it serves
// (crates/core/src/server/client_api.rs: SHELL_PAGE_CSP, sandbox_csp_for_origin).
//
// Each test below maps to a regression that previously shipped to production
// with only a Rust-level HTML-string assertion guarding it:
//   - #3842 — shell CSP must allow same-origin fetches (connect-src 'self').
//   - #3852 — cross-origin target="_blank" links must not open null-origin
//             sandboxed popups; they go through the shell's open_url bridge.
//   - #3853 / #3854 — middle-click (auxclick) and shift-click must also be
//             intercepted, not just primary-button click.

const shellUrl = process.env.FREENET_SHELL_URL;

test.beforeAll(() => {
  if (!shellUrl) {
    throw new Error(
      "FREENET_SHELL_URL is not set. These tests are normally driven by the " +
        "Rust harness (crates/core/tests/playwright_shell.rs), which boots a " +
        "node, publishes the fixture, and exports the shell URL.",
    );
  }
});

// Collector for browser console messages so individual tests can assert the
// absence of CSP violations. Chromium reports a CSP block as a console error
// whose text contains "Content Security Policy".
function trackConsole(page: Page): ConsoleMessage[] {
  const messages: ConsoleMessage[] = [];
  page.on("console", (msg) => messages.push(msg));
  return messages;
}

function cspViolations(messages: ConsoleMessage[]): string[] {
  return messages
    .map((m) => m.text())
    .filter((t) => /content security policy/i.test(t));
}

// Install a capturing listener on the SHELL (top) window that records every
// `__freenet_shell__` postMessage the iframe sends up. This lets us assert the
// exact payload shape of the open_url / navigate contract, not just its
// side effects. Must run before any interaction.
async function captureShellMessages(page: Page): Promise<void> {
  await page.evaluate(() => {
    (window as unknown as { __freenetMessages: unknown[] }).__freenetMessages = [];
    window.addEventListener(
      "message",
      (e) => {
        const d = e.data;
        if (d && typeof d === "object" && (d as { __freenet_shell__?: boolean }).__freenet_shell__) {
          (window as unknown as { __freenetMessages: unknown[] }).__freenetMessages.push(d);
        }
      },
      true,
    );
  });
}

type ShellMessage = { type: string; url?: string; href?: string; shiftKey?: boolean };

async function shellMessages(page: Page): Promise<ShellMessage[]> {
  return page.evaluate(
    () => (window as unknown as { __freenetMessages: ShellMessage[] }).__freenetMessages,
  );
}

// Override window.open on the shell page so an open_url that the bridge
// honours is observable (and so the test never actually launches a popup to
// example.com). Returns nothing; read the calls via openCalls().
async function stubWindowOpen(page: Page): Promise<void> {
  await page.evaluate(() => {
    (window as unknown as { __openCalls: string[] }).__openCalls = [];
    window.open = ((url?: string | URL) => {
      (window as unknown as { __openCalls: string[] }).__openCalls.push(String(url ?? ""));
      return null;
    }) as typeof window.open;
  });
}

async function openCalls(page: Page): Promise<string[]> {
  return page.evaluate(() => (window as unknown as { __openCalls: string[] }).__openCalls);
}

// The shell wraps the contract in an iframe#app. Wait for the iframe to load
// the fixture (its #title) and return a handle to that frame.
async function fixtureFrame(page: Page) {
  const frameElement = page.locator("iframe#app");
  await expect(frameElement).toBeAttached();
  const frame = page.frameLocator("iframe#app");
  await expect(frame.locator("#title")).toBeVisible();
  return frame;
}

test("shell page loads and embeds the sandboxed iframe", async ({ page }) => {
  const consoleMessages = trackConsole(page);
  const resp = await page.goto(shellUrl!);
  expect(resp?.ok()).toBeTruthy();

  // The shell serves its strict CSP on the outer page.
  const csp = resp?.headers()["content-security-policy"] ?? "";
  expect(csp, `shell CSP header missing: ${csp}`).toContain("frame-src 'self'");
  // connect-src must include BOTH same-origin fetch (#3842) and ws/wss.
  expect(csp).toMatch(/connect-src[^;]*'self'/);
  expect(csp).toMatch(/connect-src[^;]*ws:/);

  // The sandbox attribute must NOT grant allow-same-origin (origin isolation,
  // GHSA-824h-7x5x-wfmf) but must allow scripts + popups.
  const sandbox = await page.locator("iframe#app").getAttribute("sandbox");
  expect(sandbox, `iframe sandbox: ${sandbox}`).toContain("allow-scripts");
  expect(sandbox).toContain("allow-popups");
  expect(sandbox).not.toContain("allow-same-origin");
  expect(sandbox).not.toContain("allow-popups-to-escape-sandbox");

  await fixtureFrame(page);
  expect(
    cspViolations(consoleMessages),
    `unexpected CSP violations on initial load: ${cspViolations(consoleMessages).join(" | ")}`,
  ).toEqual([]);
});

test("same-origin permission poll fetch is allowed by the shell CSP (#3842)", async ({ page }) => {
  const consoleMessages = trackConsole(page);
  await page.goto(shellUrl!);
  const frame = await fixtureFrame(page);

  // The fixture fires `fetch('/permission/pending')` on load. Under the fixed
  // CSP (connect-src 'self') it resolves; under the #3842 regression
  // (connect-src ws: wss:) it would be blocked and the result would start with
  // "error:". The endpoint exists on the node, so a non-error result confirms
  // the fetch reached the server.
  const result = frame.locator("#poll-result");
  await expect(result).not.toHaveText("pending");
  await expect(result).toHaveText(/^fetched:/);

  expect(
    cspViolations(consoleMessages),
    `permission poll triggered a CSP violation (regression of #3842): ${cspViolations(consoleMessages).join(" | ")}`,
  ).toEqual([]);
});

test("left-click on a cross-origin link routes through the open_url bridge (#3852)", async ({
  page,
}) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  await stubWindowOpen(page);
  const frame = await fixtureFrame(page);

  await frame.locator("#cross-origin-link").click();

  // The interceptor must postMessage open_url to the shell ...
  await expect
    .poll(async () => (await shellMessages(page)).map((m) => m.type))
    .toContain("open_url");
  const msgs = await shellMessages(page);
  const openUrl = msgs.find((m) => m.type === "open_url");
  expect(openUrl?.url).toBe("https://example.com/external");
  expect(openUrl?.shiftKey).toBe(false);

  // ... and the shell must honour it via window.open with the real origin
  // (not a sandboxed null-origin popup).
  await expect.poll(async () => await openCalls(page)).toContain("https://example.com/external");
});

test("middle-click (auxclick) on a cross-origin link is also intercepted (#3853/#3854)", async ({
  page,
}) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  await stubWindowOpen(page);
  const frame = await fixtureFrame(page);

  // Middle-click dispatches `auxclick`, not `click`. Pre-#3854 only `click`
  // was listened for, so the browser opened a null-origin sandboxed popup.
  await frame.locator("#cross-origin-link").click({ button: "middle" });

  await expect
    .poll(async () => (await shellMessages(page)).map((m) => m.type))
    .toContain("open_url");
  const openUrl = (await shellMessages(page)).find((m) => m.type === "open_url");
  expect(openUrl?.url).toBe("https://example.com/external");
});

test("shift-click on a cross-origin link forwards shiftKey (#3853)", async ({ page }) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  await stubWindowOpen(page);
  const frame = await fixtureFrame(page);

  await frame.locator("#cross-origin-link").click({ modifiers: ["Shift"] });

  await expect
    .poll(async () => (await shellMessages(page)).some((m) => m.type === "open_url" && m.shiftKey === true))
    .toBeTruthy();
});

test("same-origin in-contract link performs an in-place navigate hop", async ({ page }) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  await frame.locator("#same-origin-link").click();

  // Interceptor sends a `navigate` (not `open_url`) for a same-origin link.
  await expect
    .poll(async () => (await shellMessages(page)).map((m) => m.type))
    .toContain("navigate");
  const navigate = (await shellMessages(page)).find((m) => m.type === "navigate");
  expect(navigate?.href, `navigate href: ${navigate?.href}`).toContain("page2.html");

  // The shell performs the hop in place: the iframe now shows page 2 and the
  // top-level URL no longer carries __sandbox (issue #3839). Use Playwright's
  // polling toHaveURL (not a synchronous page.url() snapshot) because the
  // pushState that updates the address bar runs in the bridge's message
  // handler, which can settle a tick after the iframe content loads.
  await expect(page.frameLocator("iframe#app").locator("#page2-title")).toBeVisible();
  await expect(page).toHaveURL(/page2\.html/);
  await expect(page).not.toHaveURL(/__sandbox/);
});

test("browser Back restores the previous subpage via the popstate handler (#3839)", async ({
  page,
}) => {
  await page.goto(shellUrl!);
  const frame = await fixtureFrame(page);

  // Navigate forward to page 2 (in-place hop, pushes a history entry).
  await frame.locator("#same-origin-link").click();
  await expect(page.frameLocator("iframe#app").locator("#page2-title")).toBeVisible();
  await expect(page).toHaveURL(/page2\.html/);

  // Browser Back must fire the bridge's popstate handler, which restores the
  // iframe to the PREVIOUS subpage (index) rather than leaving it on page 2 or
  // blanking it. Restoring iframe.src is exactly the behaviour the popstate
  // handler owns (path_handlers.rs SHELL_BRIDGE_JS popstate listener), so we
  // assert on the iframe content — the observable effect of that handler.
  //
  // We deliberately do NOT assert on the top-level address-bar URL here: the
  // forward hop used history.pushState, so going back is a popstate event that
  // only swaps iframe.src and never triggers a document load. Whether/when the
  // browser's address bar reverts on a scripted history.back() is a browser
  // history detail, not part of the bridge's contract, and asserting it is
  // flaky under headless automation. The forward-navigate test above already
  // pins the address-bar behaviour for the push direction.
  await page.evaluate(() => window.history.back());
  await expect(page.frameLocator("iframe#app").locator("#title")).toBeVisible();
  await expect(page.frameLocator("iframe#app").locator("#page2-title")).toHaveCount(0);
});
