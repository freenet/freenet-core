import { test, expect, type Page, type Request } from "@playwright/test";

// Regression tests for freenet-core#5213: opening several Freenet apps at once
// made every FURTHER tab hang with no error — the browser simply never issued
// the document request. Closing tabs unstuck the pending one.
//
// Mechanism. Every Freenet app is served from the SAME origin (the node's
// local HTTP listener, default 127.0.0.1:7509), and browsers cap concurrent
// HTTP/1.1 connections per origin at about six — Chrome 6, Firefox 6, WebKit
// 6. The shell page subscribed to permission prompts with an EventSource,
// which is a normal HTTP request held open for the lifetime of the tab. Six
// open tabs therefore consumed the entire per-origin budget permanently, and
// the seventh tab's document request queued behind them forever. The fix moves
// that subscription to a WebSocket, which browsers pool separately (~255 per
// profile in Chrome, 200 in Firefox) and which does not draw from the HTTP
// budget.
//
// Why this lives in Playwright and not in Rust. The bug is entirely a property
// of the BROWSER's connection scheduler. The server was healthy throughout: it
// accepted every connection offered to it, and every Rust-level test passed,
// because no Rust test has a per-origin connection budget. The only thing that
// can observe this failure is a real browser opening real tabs. Rust-side
// tests can pin the shape of the emitted JS (path_handlers.rs does), but a
// substring assertion cannot tell an EventSource apart from any OTHER way of
// holding an HTTP request open — a streamed fetch() or a long-poll would
// reintroduce #5213 and keep those assertions green. The first test below
// checks the actual invariant instead of one spelling of its violation.

const shellUrl = process.env.FREENET_SHELL_URL;

// One more than the ~6-connections-per-origin budget every current engine
// enforces, so a held-open-request regression stalls INSIDE this range rather
// than just short of it. Well under MAX_PERMISSION_SUBSCRIBERS (64), so the
// server-side cap is not what is under test here.
const TAB_COUNT = 8;

// A request still unfinished this long after the page has loaded is being HELD
// open, not merely slow: the node is on loopback and the contract bundle is
// already warm by the time these tests run. Generous enough to absorb a
// contended CI runner without turning a slow fetch into a failure.
const HELD_OPEN_AFTER_MS = 5_000;

test.beforeAll(() => {
  if (!shellUrl) {
    throw new Error(
      "FREENET_SHELL_URL is not set. These tests are normally driven by the " +
        "Rust harness (crates/core/tests/playwright_shell.rs), which boots a " +
        "node, publishes the fixture, and exports the shell URL.",
    );
  }
});

// Opening TAB_COUNT tabs and loading a contract bundle in each is well past
// the 60s default, especially across three engines on a shared runner.
test.describe.configure({ timeout: 180_000 });

// Track every request the page issues and which of them have completed, so a
// request that is being held open is distinguishable from one that finished.
function trackInFlight(page: Page): {
  inFlight: () => Request[];
} {
  const started = new Map<Request, number>();
  page.on("request", (r) => started.set(r, Date.now()));
  page.on("requestfinished", (r) => started.delete(r));
  page.on("requestfailed", (r) => started.delete(r));
  return {
    inFlight: () => [...started.keys()],
  };
}

// Record the permission-channel WebSocket each page opens. This is the
// positive control for the tab tests below: without it they could pass simply
// because the shell subscribed to nothing at all, which would hold no
// connection and prove nothing about the fix.
function trackPermissionSockets(page: Page): { urls: string[] } {
  const urls: string[] = [];
  page.on("websocket", (ws) => {
    if (ws.url().includes("/permission/events")) {
      urls.push(ws.url());
    }
  });
  return { urls };
}

test("the shell holds no HTTP request open for the life of the tab (#5213)", async ({
  page,
}) => {
  const tracker = trackInFlight(page);
  const sockets = trackPermissionSockets(page);

  await page.goto(shellUrl!);
  await page.waitForLoadState("load");

  // Give anything held-open time to become distinguishable from anything
  // merely slow, and give the permission subscription time to be established.
  await page.waitForTimeout(HELD_OPEN_AFTER_MS);

  const stuck = tracker.inFlight().map((r) => `${r.method()} ${r.url()}`);
  expect(
    stuck,
    "the shell must not hold any HTTP request open: each one permanently " +
      "consumes one of the browser's ~6 connections per origin, and all " +
      "Freenet apps share the node's single origin (#5213). This is the " +
      "invariant, not merely the absence of `new EventSource(` — a streamed " +
      "fetch() or a long-poll would break it the same way.",
  ).toEqual([]);

  // Non-vacuity: the permission channel must actually exist. If it did not,
  // the assertion above would pass for the wrong reason.
  expect(
    sockets.urls,
    "the shell must subscribe to permission prompts over a WebSocket, which " +
      "browsers pool separately from the per-origin HTTP budget",
  ).not.toEqual([]);
});

test(`${TAB_COUNT} Freenet tabs can be open at once and a ${TAB_COUNT + 1}th still loads (#5213)`, async ({
  context,
}) => {
  const pages: Page[] = [];
  const socketTrackers: Array<{ urls: string[] }> = [];

  // Same BrowserContext for every tab: the per-origin connection budget is a
  // property of the profile, so separate contexts would each get their own
  // budget and the exhaustion could never occur.
  for (let i = 0; i < TAB_COUNT; i++) {
    const page = await context.newPage();
    pages.push(page);
    socketTrackers.push(trackPermissionSockets(page));

    // Before the fix this call never returns for i >= 6: the document request
    // is queued behind six held-open EventSource requests and the browser
    // surfaces no error, so the timeout IS the reproduction.
    const resp = await page.goto(shellUrl!, { timeout: 30_000 });
    expect(resp, `tab ${i + 1} produced no response`).not.toBeNull();
    expect(resp!.ok(), `tab ${i + 1} failed to load`).toBe(true);
    await page.waitForLoadState("load");
  }

  // Every tab must be holding a live permission subscription. Without this the
  // test would also pass against a shell that subscribes to nothing, which
  // holds no connection and says nothing about #5213.
  for (let i = 0; i < TAB_COUNT; i++) {
    await expect
      .poll(() => socketTrackers[i].urls.length, {
        message: `tab ${i + 1} never opened a permission WebSocket, so this run proves nothing about connection exhaustion`,
        timeout: 15_000,
      })
      .toBeGreaterThan(0);
  }

  // The user-visible symptom was "new links stop loading" — a NEW navigation
  // while the other tabs stay open. Assert that directly, not just that the
  // first TAB_COUNT tabs happened to load.
  const extra = await context.newPage();
  const resp = await extra.goto(shellUrl!, { timeout: 30_000 });
  expect(
    resp?.ok(),
    `a new tab must still load with ${TAB_COUNT} Freenet tabs already open (#5213)`,
  ).toBe(true);

  for (const page of [...pages, extra]) {
    await page.close();
  }
});
