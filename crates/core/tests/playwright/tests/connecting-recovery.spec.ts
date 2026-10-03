import { test, expect, type Page } from "@playwright/test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

// Serve the actual 503 asset: this exercises navigation and <noscript> in each
// browser without relying on a node happening to be mid-rejoin.
const html = readFileSync(
  join(__dirname, "../../../src/server/errors/assets/connecting.html"),
  "utf8",
);
const origin = "http://connecting.test";
const appPath = "/v1/contract/web/shared-key/";
const now = 5_000_000;

test.beforeEach(async ({ page }) => {
  await page.route("**/*", async (route) => {
    const url = new URL(route.request().url());
    if (url.origin !== origin) return route.abort();
    await route.fulfill({
      status: url.pathname === "/" ? 200 : 503,
      contentType: "text/html",
      body: url.pathname === "/" ? "<h1>Dashboard</h1>" : html,
    });
  });
});

test("shared app link stamps recovery and retries the same URL", async ({
  page,
}) => {
  await page.clock.install({ time: now - 60_000 });
  await page.clock.pauseAt(now);
  await page.goto(`${origin}${appPath}?view=chat#message`);
  await expect(page.locator("#msg")).toContainText("Your peer is reconnecting");
  await page.clock.runFor(3_000);
  await expect(page).toHaveURL(
    `${origin}${appPath}?view=chat&_freload=${now + 3_000}-0#message`,
  );
  await page.waitForLoadState();
  const stamped = page.url();
  const reloaded = page.waitForRequest(stamped.split("#")[0]);
  await page.clock.runFor(3_000);
  await reloaded;
  await expect(page).toHaveURL(stamped);
});

test("a v2 shared app link also stamps recovery instead of leaving", async ({
  page,
}) => {
  await page.clock.install({ time: now - 60_000 });
  await page.clock.pauseAt(now);
  await page.goto(`${origin}/v2/contract/web/shared-key/`);
  await page.clock.runFor(3_000);
  await expect(page).toHaveURL(
    `${origin}/v2/contract/web/shared-key/?_freload=${now + 3_000}-0`,
  );
});

test("expired top-level app recovery falls back to the dashboard", async ({
  page,
}) => {
  await page.clock.install({ time: now - 60_000 });
  await page.clock.pauseAt(now);
  await page.goto(`${origin}${appPath}?_freload=${now - 120_001}-0`);
  await page.clock.runFor(3_000);
  await expect(page).toHaveURL(`${origin}/`);
  await expect(page.locator("h1")).toHaveText("Dashboard");
});

test("unmarked top-level non-app path still goes to the dashboard", async ({
  page,
}) => {
  await page.clock.install({ time: now - 60_000 });
  await page.clock.pauseAt(now);
  await page.goto(`${origin}/some/other/path`);
  await page.clock.runFor(3_000);
  await expect(page).toHaveURL(`${origin}/`);
});

test.describe("without JavaScript", () => {
  test.use({ javaScriptEnabled: false });

  // Unchanged by this fix: the <noscript> meta keeps the dashboard fallback
  // (an app cannot run without JavaScript anyway).
  test("connecting page falls back to the dashboard", async ({ page }) => {
    await page.goto(`${origin}${appPath}`);
    await expect(page.locator('meta[http-equiv="refresh"]')).toHaveCount(1);
    await page.waitForURL(`${origin}/`, { timeout: 6_000 });
  });
});

// Once the shell is served, the recovery stamp leaves the address bar: a link a
// person copies from it later is a FRESH link. A stale stamp would send that
// link, opened top-level on a node still joining, straight to the dashboard
// (the expired-window branch above). The shell keeps a LIVE reload-cap window
// (count >= 1) until it expires, so its loop bound is unchanged.
test.describe("after a recovered load", () => {
  // The node's real shell page (path_handlers.rs renders shell.html with
  // `format!`): the same template and bridge, non-hosted, one-arg call.
  const read = (p: string) =>
    readFileSync(
      join(__dirname, "../../../src/server/path_handlers/assets", p),
      "utf8",
    );
  const values: Record<string, string> = {
    favicon: "",
    hosted_styles: "",
    hosted_bar: "",
    iframe_src: `${appPath}?__sandbox=1`,
    SHELL_BRIDGE_JS: read("shell_bridge.js"),
    user_token_script: "",
    bridge_call: 'freenetBridge("tok");',
  };
  const shell = read("shell.html").replace(/\{\{|\}\}|\{(\w+)\}/g, (m, name) =>
    m === "{{" ? "{" : m === "}}" ? "}" : values[name],
  );
  // The node has joined: the app link answers with its shell, and the
  // sandboxed frame with the app.
  const joined = async (page: Page) =>
    page.route(`${origin}${appPath}*`, (route) => {
      const sandboxed = new URL(route.request().url()).searchParams.has(
        "__sandbox",
      );
      return route.fulfill({
        status: 200,
        contentType: "text/html",
        body: sandboxed ? "<p>app</p>" : shell,
      });
    });

  test("the address bar drops the recovery stamp, and that link later takes the fresh path", async ({
    page,
  }) => {
    await page.clock.install({ time: now - 60_000 });
    await page.clock.pauseAt(now);
    await joined(page);
    await page.goto(
      `${origin}${appPath}?view=chat&_freload=${now - 5_000}-0#message`,
    );
    await page.clock.runFor(1);
    await expect(page).toHaveURL(`${origin}${appPath}?view=chat#message`);
    const copied = page.url();

    // An hour later, the copied link opened in a new tab, on a node that is
    // joining again (the page's own URL would be a same-document navigation).
    await page.clock.pauseAt(now + 3_600_000);
    const later = await page.context().newPage();
    await later.route("**/*", (route) =>
      route.fulfill({ status: 503, contentType: "text/html", body: html }),
    );
    await later.goto(copied);
    await page.clock.runFor(3_000);
    await expect(later).toHaveURL(
      `${origin}${appPath}?view=chat&_freload=${now + 3_603_000}-0#message`,
    );
  });

  test("a live reload-cap window is kept until it expires, then dropped", async ({
    page,
  }) => {
    await page.clock.install({ time: now - 60_000 });
    await page.clock.pauseAt(now);
    await joined(page);
    const capped = `${origin}${appPath}?_freload=${now - 5_000}-2`;
    await page.goto(capped);
    await page.clock.runFor(54_000);
    await expect(page).toHaveURL(capped);
    await page.clock.runFor(2_000);
    await expect(page).toHaveURL(`${origin}${appPath}`);
  });
});
