import {
  test,
  expect,
  type BrowserContext,
  type Page,
  type ConsoleMessage,
} from "@playwright/test";

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
//             sandboxed popups. They now open natively, and
//             `allow-popups-to-escape-sandbox` is what gives them a real origin.
//   - #3854 — middle-click (auxclick) must be classified like click.
//   - #5087 — same-origin target="_blank" (i.e. every cross-CONTRACT link) must
//             open a working tab. Its first fix routed the click through the
//             shell's open_url bridge, which put window.open inside a `message`
//             handler — blocked by Firefox's popup blocker, so the link died
//             there while Chrome/Safari kept working.

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
    (window as unknown as { __freenetMessages: unknown[] }).__freenetMessages =
      [];
    window.addEventListener(
      "message",
      (e) => {
        const d = e.data;
        if (
          d &&
          typeof d === "object" &&
          (d as { __freenet_shell__?: boolean }).__freenet_shell__
        ) {
          (
            window as unknown as { __freenetMessages: unknown[] }
          ).__freenetMessages.push(d);
        }
      },
      true,
    );
  });
}

type ShellMessage = {
  type: string;
  url?: string;
  href?: string;
  shiftKey?: boolean;
};

async function shellMessages(page: Page): Promise<ShellMessage[]> {
  const all = await page.evaluate(
    () =>
      (window as unknown as { __freenetMessages: ShellMessage[] })
        .__freenetMessages,
  );
  // The injected title-sync script posts a `title` message on every
  // sandboxed-page load (and re-fires on each in-place navigate hop), fully
  // independent of user interaction. Every call site of this helper asserts
  // the exact sequence of CLICK-classification messages (navigate/open_url),
  // so a `title` landing anywhere in that sequence is page-lifecycle noise,
  // not a signal any of them are testing for — filter it out here rather
  // than at each call site.
  return all.filter((m) => m.type !== "title");
}

// Serve the RFC 2606 documentation domain from the test itself, so the tabs
// these tests open resolve instantly and offline. The point of the assertions
// is that a real top-level document opens with a real origin — not that
// example.com is reachable from CI.
async function stubExternal(context: BrowserContext): Promise<void> {
  await context.route("https://example.com/**", (route) =>
    route.fulfill({
      contentType: "text/html",
      body: "<title>external</title>ok",
    }),
  );
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
  // Popups MUST escape the sandbox. Without it a new tab inherits the opaque
  // origin and dead-ends (blank shell, no localStorage for the hosted access
  // key), and the workaround — letting the shell open the tab from a
  // `message` handler — is refused by Firefox's popup blocker.
  expect(sandbox).toContain("allow-popups-to-escape-sandbox");

  await fixtureFrame(page);
  expect(
    cspViolations(consoleMessages),
    `unexpected CSP violations on initial load: ${cspViolations(consoleMessages).join(" | ")}`,
  ).toEqual([]);
});

test("same-origin permission poll fetch is allowed by the shell CSP (#3842)", async ({
  page,
}) => {
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

test('cross-origin target="_blank" opens natively as a real top-level tab (#3852, river#208)', async ({
  page,
  context,
}) => {
  await stubExternal(context);
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  const opened = context.waitForEvent("page");
  await frame.locator("#cross-origin-link").click();
  const popup = await opened;
  await popup.waitForLoadState();

  // A REAL top-level document, not a sandbox-inheriting popup: the stub route
  // served it, and its origin is example.com rather than "null" (the
  // null-origin popup is what broke CORS on logged-in sites in river#208).
  expect(popup.url()).toBe("https://example.com/external");
  expect(await popup.evaluate(() => location.origin)).toBe(
    "https://example.com",
  );
  await popup.close();

  // The click must NOT have been handed to the shell: `window.open` from a
  // `message` handler is popup-blocked in Firefox, which is what made these
  // links dead there while Chrome/Safari kept working.
  expect((await shellMessages(page)).map((m) => m.type)).toEqual([]);
});

test('middle-click on a cross-origin target="_blank" link also opens a tab (#3854)', async ({
  page,
  context,
}) => {
  await stubExternal(context);
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  // Middle-click dispatches `auxclick`, not `click`. The interceptor listens on
  // both so the classification is identical; a targeted link stays native
  // either way, and the browser gives it real background-tab placement — which
  // the old postMessage route could not preserve.
  const opened = context.waitForEvent("page");
  await frame.locator("#cross-origin-link").click({ button: "middle" });
  const popup = await opened;
  // A middle-click opens a BACKGROUND tab, which starts at about:blank and
  // commits its navigation later than a foreground one — `waitForLoadState()`
  // can resolve on the initial empty document. Wait for the URL itself.
  await popup.waitForURL("https://example.com/external");
  await popup.close();
  expect((await shellMessages(page)).map((m) => m.type)).toEqual([]);
});

test("cross-origin link with NO target is opened in a tab by the interceptor", async ({
  page,
  context,
}) => {
  await stubExternal(context);
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  // Left native this would navigate the app frame to a foreign origin, which
  // the shell's `frame-src 'self'` refuses — a dead click. The interceptor
  // calls window.open itself, inside the click handler, so the gesture is live
  // (Firefox allows window.open from `click`, never from `message`).
  const opened = context.waitForEvent("page");
  await frame.locator("#cross-origin-untargeted-link").click();
  const popup = await opened;
  await popup.waitForLoadState();
  expect(popup.url()).toBe("https://example.com/plain");
  await popup.close();

  // Still no shell round-trip, and the app frame stayed put.
  expect((await shellMessages(page)).map((m) => m.type)).toEqual([]);
  await expect(page.frameLocator("iframe#app").locator("#title")).toBeVisible();
});

test('same-origin target="_blank" opens a working new tab, not a blank one (#5087)', async ({
  page,
  context,
}) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  // The regression this pins, in both its forms:
  //   - Before #5087 the popup inherited the sandbox, so the shell it landed on
  //     had an opaque origin, its `frame-src 'self'` could not match, and the
  //     app frame stayed about:blank — a blank tab.
  //   - #5087 routed the click through the shell's `open_url` bridge, which put
  //     `window.open` inside a `message` handler: blocked outright by Firefox's
  //     popup blocker, so the click did nothing at all there.
  // With `allow-popups-to-escape-sandbox` the native popup is a real top-level
  // document and the shell inside it loads the contract normally.
  const opened = context.waitForEvent("page");
  await frame.locator("#same-origin-blank-link").click();
  const popup = await opened;
  await popup.waitForLoadState();

  expect(popup.url()).toContain("page2.html");
  // Not a blank tab: the new tab rendered its own shell + app frame.
  await expect(popup.locator("iframe#app")).toBeAttached();
  await expect(
    popup.frameLocator("iframe#app").locator("#page2-title"),
  ).toBeVisible();
  await popup.close();

  expect((await shellMessages(page)).map((m) => m.type)).toEqual([]);
});

test("contract JS calling window.open() gets a real top-level tab", async ({
  page,
  context,
}) => {
  await stubExternal(context);
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  // The anchor interceptor never sees this — the app calls window.open() from
  // its own script. Before #5100 an override forwarded it to the shell, because
  // a popup from the sandboxed frame inherited the sandbox: opaque origin, no
  // localStorage, dead-ending on the per-user-isolation page (#4645). With
  // `allow-popups-to-escape-sandbox` the native call is enough, so the override
  // was removed — this test is what proves removing it was safe.
  const opened = context.waitForEvent("page");
  await frame.locator("#programmatic-open").click();
  const popup = await opened;
  await popup.waitForURL("https://example.com/programmatic");

  // A real top-level context, not a sandbox-inheriting one: a genuine origin
  // (never "null") and working storage, which is what #4645 was about.
  expect(await popup.evaluate(() => location.origin)).toBe(
    "https://example.com",
  );
  expect(
    await popup.evaluate(() => {
      try {
        localStorage.setItem("__probe", "1");
        localStorage.removeItem("__probe");
        return "usable";
      } catch (e) {
        return "denied";
      }
    }),
    "an opaque-origin (sandbox-inherited) tab throws on localStorage — that dead end is #4645",
  ).toBe("usable");
  await popup.close();

  // No shell round-trip: nothing was forwarded.
  expect((await shellMessages(page)).map((m) => m.type)).toEqual([]);
});

// Does this document hold the node's real origin, or an opaque one? Storage is
// the sharpest available test: an opaque origin throws SecurityError on any
// `localStorage` access, and the node's origin is where the hosted per-user
// access key lives.
const STORAGE_PROBE = () => {
  try {
    window.localStorage.setItem("__probe", "1");
    window.localStorage.removeItem("__probe");
    return "NODE-ORIGIN";
  } catch (e) {
    return "OPAQUE";
  }
};

test("a contract cannot reach a node-origin context by escaping the sandbox (#3818)", async ({
  page,
}) => {
  await page.goto(shellUrl!);
  // Wait for the fixture to render before reaching for the Frame handle: the
  // FrameLocator helper is what knows how to wait, but only a Frame can
  // `evaluate`.
  await fixtureFrame(page);
  const appFrame = page.frames().find((f) => f.url().includes("__sandbox=1"));
  expect(appFrame, "the app frame must be loaded").toBeTruthy();

  // Controls first, so a probe that stopped distinguishing anything shows up
  // here rather than as a silent pass below.
  expect(
    await page.evaluate(STORAGE_PROBE),
    "the shell IS the node origin — if this is not NODE-ORIGIN the probe is broken, not the sandbox",
  ).toBe("NODE-ORIGIN");
  expect(
    await appFrame!.evaluate(STORAGE_PROBE),
    "the app frame must be opaque-origin",
  ).toBe("OPAQUE");

  // `allow-popups-to-escape-sandbox` is load-bearing for the new-tab fix, and
  // it hands a contract an unsandboxed top-level context it can script (the
  // `about:blank` popup inherits this frame's origin). The sandbox ATTRIBUTE
  // cannot reach what happens in there. What must hold is that the contract's
  // own bytes are still opaque-origin when re-embedded from it, because the
  // server sandboxes them itself (CONTRACT_CONTENT_SANDBOX_CSP).
  //
  // Without that header this reproduces in chromium, firefox and webkit: the
  // nested frame reports NODE-ORIGIN, and from there localStorage yields the
  // hosted access key and same-origin fetch yields another app's auth token.
  const [popup] = await Promise.all([
    page.waitForEvent("popup"),
    appFrame!.click("#escape-sandbox"),
  ]);
  const escape = await appFrame!.evaluate(() => ({
    opened: !!(window as unknown as { __escapeOpened?: boolean }).__escapeOpened,
    wrote: !!(window as unknown as { __escapeWrote?: boolean }).__escapeWrote,
    error:
      (window as unknown as { __escapeWriteError?: string })
        .__escapeWriteError ?? null,
  }));
  expect(
    escape.opened,
    "the popup must open, or this test is not exercising the escape at all",
  ).toBe(true);

  // Give the nested frame time to load before enumerating. It may legitimately
  // never appear: some engines refuse the cross-document write once the opener
  // is itself CSP-sandboxed, which blocks the escape one step earlier.
  await popup.waitForTimeout(1000);
  const nested = popup.frames().filter((f) => f !== popup.mainFrame());
  const reports: string[] = [];
  for (const f of nested) {
    reports.push(
      await f
        .evaluate(STORAGE_PROBE)
        .catch((e) => `UNREACHABLE: ${String(e).split("\n")[0]}`),
    );
  }
  const detail = `escape=${JSON.stringify(escape)} frames=${JSON.stringify(
    nested.map((f) => f.url()),
  )} probes=${JSON.stringify(reports)}`;

  if (escape.wrote) {
    // The write landed, so the nested frames are the thing under test and must
    // actually be there — otherwise the assertion below passes on an empty set.
    // BOTH must be probed: the plain asset route and the `?__sandbox=1` route
    // are guarded in different places, and the second one is the sharper attack
    // (the contract's own page, no exotic type needed).
    expect(
      nested.length,
      `the escaped popup accepted the write but embedded fewer frames than it asked for, so a route went unprobed: ${detail}`,
    ).toBe(2);
  }
  expect(
    reports,
    `contract content reached the node's own origin from an escaped popup: ${detail}`,
  ).not.toContain("NODE-ORIGIN");
  // `not.toContain` alone also passes on "UNREACHABLE: …", which is what a
  // frame that 404s or never loads produces — so a broken fixture would read as
  // a security pass. Require the positive result whenever a frame is there.
  if (nested.length > 0) {
    expect(
      reports,
      `every embedded frame must actually be probed and report an opaque origin: ${detail}`,
    ).toEqual(nested.map(() => "OPAQUE"));
  }

  await popup.close();
});

test("a modifier-click on an untargeted in-contract link opens a real tab", async ({
  page,
  context,
}) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  // `#same-origin-link` carries no `target`, so it used to reach the
  // same-origin branch on EVERY click and become an in-place `navigate` hop —
  // middle-click and ctrl/cmd-click included, which silently lost their "open
  // in a new tab" meaning. The modifier skip now runs before origin
  // classification, so the browser gets the click and gives it real
  // background-tab placement.
  const opened = context.waitForEvent("page");
  await frame.locator("#same-origin-link").click({ button: "middle" });
  const popup = await opened;
  // Background tabs commit their navigation late; wait for the URL, not the
  // initial empty document.
  await popup.waitForURL(/page2\.html/);
  // A real top-level tab, not a blank sandbox-inheriting one: it renders its
  // own shell and app frame.
  await expect(popup.locator("iframe#app")).toBeAttached();
  await expect(
    popup.frameLocator("iframe#app").locator("#page2-title"),
  ).toBeVisible();
  await popup.close();

  expect(
    (await shellMessages(page)).map((m) => m.type),
    "a modifier-click must not be turned into an in-place navigate hop",
  ).toEqual([]);

  // Shift-click is the other half of #3853. Its e2e coverage on main asserted
  // the interceptor FORWARDED shiftKey to the shell; that route is gone, so the
  // contract to pin is that the browser gets the click. Kept here rather than
  // deleted with the old spec.
  const shiftOpened = context.waitForEvent("page");
  await frame
    .locator("#same-origin-link")
    .click({ modifiers: ["Shift"] });
  const shiftWindow = await shiftOpened;
  await shiftWindow.waitForURL(/page2\.html/);
  await shiftWindow.close();
  expect(
    (await shellMessages(page)).map((m) => m.type),
    "shift-click must reach the browser too, so it can place a new WINDOW",
  ).toEqual([]);

  // Control, in the SAME document: an UNMODIFIED click on the very same link IS
  // intercepted. Without this the assertion above would also pass if the
  // interceptor had simply stopped running.
  await frame.locator("#same-origin-link").click();
  await expect
    .poll(async () => (await shellMessages(page)).map((m) => m.type), {
      message:
        "the plain click on the same link must still produce the navigate hop",
    })
    .toEqual(["navigate"]);
});

test('target="_top" on a cross-origin link is not a dead click', async ({
  page,
  context,
}) => {
  await stubExternal(context);
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  // `_top` and `_parent` name an ANCESTOR context. The sandbox has no
  // `allow-top-navigation`, so left to the browser the click does nothing at
  // all — measured in chromium and firefox, where `main` opened a tab. They
  // must therefore fall through to origin classification and be opened like an
  // untargeted cross-origin link.
  const opened = context.waitForEvent("page");
  await frame.locator("#cross-origin-top-link").click();
  const popup = await opened;
  await popup.waitForLoadState();
  expect(popup.url()).toBe("https://example.com/top");
  await popup.close();

  // The app frame is untouched, and nothing went through the shell.
  await expect(frame.locator("#title")).toBeVisible();
  expect((await shellMessages(page)).map((m) => m.type)).toEqual([]);
});

test("right-click is not intercepted: no stray tab, no frame navigation", async ({
  page,
  context,
}) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  // `auxclick` fires for the SECONDARY button as well as the middle one, and
  // `preventDefault` on it does not suppress the context menu (that comes from
  // mousedown). So an interceptor that skips only `e.button === 1` gives the
  // user the menu AND a side effect: a stray tab for a cross-origin link, an
  // app-frame navigation for a same-origin one. Measured in chromium and
  // firefox before the fix.
  //
  // Right-clicking a link is also precisely the workaround users adopted while
  // `target="_blank"` was broken, so it is the last click that should misbehave.
  const strayTabs: string[] = [];
  context.on("page", (p) => strayTabs.push(p.url()));

  await frame.locator("#cross-origin-untargeted-link").click({ button: "right" });
  await frame.locator("#same-origin-link").click({ button: "right" });
  await page.waitForTimeout(500);

  expect(strayTabs, "right-click must not open anything").toEqual([]);
  expect(
    (await shellMessages(page)).map((m) => m.type),
    "right-click must not be intercepted at all",
  ).toEqual([]);
  // The app frame is still showing the fixture — a `navigate` hop would have
  // replaced it with page2.
  await expect(frame.locator("#title")).toBeVisible();

  // Control, same document: a plain LEFT click on the same same-origin link IS
  // intercepted, so the emptiness above is about the button, not a dead listener.
  await frame.locator("#same-origin-link").click();
  await expect
    .poll(async () => (await shellMessages(page)).map((m) => m.type))
    .toEqual(["navigate"]);
});

test("same-origin in-contract link performs an in-place navigate hop", async ({
  page,
}) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  await frame.locator("#same-origin-link").click();

  // Interceptor sends a `navigate` (not `open_url`) for a same-origin link.
  await expect
    .poll(async () => (await shellMessages(page)).map((m) => m.type))
    .toContain("navigate");
  const navigate = (await shellMessages(page)).find(
    (m) => m.type === "navigate",
  );
  expect(navigate?.href, `navigate href: ${navigate?.href}`).toContain(
    "page2.html",
  );

  // The shell performs the hop in place: the iframe now shows page 2 and the
  // top-level URL no longer carries __sandbox (issue #3839). Use Playwright's
  // polling toHaveURL (not a synchronous page.url() snapshot) because the
  // pushState that updates the address bar runs in the bridge's message
  // handler, which can settle a tick after the iframe content loads.
  await expect(
    page.frameLocator("iframe#app").locator("#page2-title"),
  ).toBeVisible();
  await expect(page).toHaveURL(/page2\.html/);
  await expect(page).not.toHaveURL(/__sandbox/);
});

test("a link carrying a download attribute is NOT intercepted", async ({
  page,
}) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  // `#download-link` is a SAME-ORIGIN href (page2.html) that, WITHOUT the
  // `download` attribute, would be intercepted as a `navigate` (proven by the
  // same-origin test above). `handleAnchorClick` early-returns on the
  // `download` attribute (path_handlers.rs:2013), so clicking it must send NO
  // interception postMessage (no `navigate`, no `open_url`). A regression that
  // dropped the `if (target.hasAttribute('download')) return;` guard would turn
  // this back into a `navigate`.
  //
  // Ordering matters: we assert on the *delta* of captured messages around the
  // click, because the click may (natively) cause the iframe to load page2 —
  // which would tear down the in-iframe listeners and make a post-click control
  // click impossible. So we (1) prove the interceptor is live with a control
  // click on the SAME element with its `download` attribute removed, expecting
  // a `navigate`; then (2) reload, re-arm capture, click the unmodified
  // download link, and assert it adds NO message.

  // (1) Control: same element minus `download` IS intercepted → listener live,
  // and the element/selector themselves are wired correctly.
  await frame
    .locator("#download-link")
    .evaluate((el) => el.removeAttribute("download"));
  await frame.locator("#download-link").click();
  await expect
    .poll(async () => (await shellMessages(page)).map((m) => m.type))
    .toContain("navigate");

  // (2) Reload to restore the original DOM (with the `download` attribute) and
  // a fresh, empty capture buffer, then click the real download link.
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame2 = await fixtureFrame(page);
  await frame2.locator("#download-link").click();
  // Give any (erroneous) interception postMessage a tick to arrive.
  await page.waitForTimeout(300);
  const types = (await shellMessages(page)).map((m) => m.type);
  expect(
    types,
    `a download link must not be intercepted, but got messages: ${types.join(", ")}`,
  ).toEqual([]);
});

test("same-origin link with target=_blank is NOT intercepted (#5106)", async ({ page }) => {
  await page.goto(shellUrl!);
  await captureShellMessages(page);
  const frame = await fixtureFrame(page);

  // `#same-origin-blank-link` is the SAME href as `#same-origin-link`
  // (page2.html) plus `target="_blank"`. The interceptor early-returns on a
  // non-`_self` target, so the click must (a) open a real tab, because the
  // browser handles it, and (b) produce NO interception postMessage.
  //
  // BOTH halves are load-bearing, and asserting only (b) was the first version
  // of this test's mistake. A regression of the shape
  //
  //     if (target.target && target.target !== '_self') { e.preventDefault(); return; }
  //
  // — cancel, forward nothing — posts no message either, so (b) alone stays
  // green on it. That shape IS #5106's user-visible symptom: the dead click.
  // (a) is what actually catches it.
  //
  // Conversely (b) is what catches #5089, which posted `open_url` here. On a
  // real local node the shell's open_url handler refuses the loopback host and
  // drops it, so nothing opened; this harness's node binds `127.x.y.1`, which
  // is NOT in that refusal list (#4846), so under #5089 a tab would still open
  // here and (a) alone would not distinguish it. Neither assertion subsumes
  // the other — keep both.
  //
  // Unlike the download-link test above, no reload is needed between the
  // subject click and its control: `target="_blank"` opens a NEW browsing
  // context, so this document and its injected listener survive intact. Doing
  // both clicks in ONE document is strictly stronger — it proves the listener
  // was live in the very document the negative assertion is made about.

  // (1) The subject: the interceptor must NOT cancel the click, so the click
  // reaches the browser and a popup is created.
  //
  // Read this as "the interceptor did not preventDefault", not as "a real user
  // gets a tab" — Playwright leaves Chromium's popup blocker off, so a popup
  // here does not establish what a blocker-on user sees. Whether the browser
  // ultimately grants the tab is the browser's business; what this pins is that
  // the decision was left to it.
  const [popup] = await Promise.all([
    page.waitForEvent("popup"),
    frame.locator("#same-origin-blank-link").click(),
  ]);
  await popup.waitForLoadState("domcontentloaded");
  expect(
    popup.url(),
    `target="_blank" must reach the browser and resolve to the link's own href; got ${popup.url()}`,
  ).toContain("page2.html");

  // (2) Control, in the SAME document: the same href WITHOUT a new-window
  // target is intercepted as `navigate`. This is a happens-after barrier for
  // the subject click too — any postMessage it wrongly sent is a message the
  // shell received before this one, so the exact-equality below cannot pass by
  // simply having raced ahead of a late delivery.
  await frame.locator("#same-origin-link").click();
  await expect
    .poll(async () => (await shellMessages(page)).map((m) => m.type), {
      message:
        'exactly one message expected: the control click\'s `navigate`. An extra `open_url` means the target="_blank" click was intercepted (#5089); no messages at all means the interceptor never ran, so the negative half of this test would have been vacuous',
    })
    .toEqual(["navigate"]);
});

test("browser Back restores the previous subpage via the popstate handler (#3839)", async ({
  page,
}) => {
  await page.goto(shellUrl!);
  const frame = await fixtureFrame(page);

  // Navigate forward to page 2 (in-place hop, pushes a history entry).
  await frame.locator("#same-origin-link").click();
  await expect(
    page.frameLocator("iframe#app").locator("#page2-title"),
  ).toBeVisible();
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
  await expect(
    page.frameLocator("iframe#app").locator("#page2-title"),
  ).toHaveCount(0);
});

test("browser tab title reflects the contract's own <title>, with no bespoke sender required", async ({
  page,
}) => {
  // The shell page's <title> is hardcoded ("Freenet") because the sandboxed
  // iframe has no allow-same-origin and cannot touch document.title on the
  // parent directly. fixture-webapp/index.html sets its own <title> and
  // deliberately implements NO shell postMessage sender (see the comment at
  // the top of that file) — the injected title-sync script
  // (path_handlers.rs TITLE_SYNC_JS) must be what carries it to the tab.
  await page.goto(shellUrl!);
  await fixtureFrame(page);
  await expect(page).toHaveTitle("Freenet shell smoke-test fixture");
});
