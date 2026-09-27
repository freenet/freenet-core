import { test, expect } from "@playwright/test";
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
