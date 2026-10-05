import { test, expect, type Page } from "@playwright/test";

// The dashboard's /routing page and the per-peer page's browser behaviour
// (#5794).
//
// Why Playwright. Rust tests render these pages as strings; they cannot see
// whether a link navigates, whether a phone scrolls sideways, or what survives
// the dashboard's auto-refresh, which fetches the page every five seconds and
// replaces <main> wholesale (dashboard.js, fetchAndSwapDashboard). Anything a
// reader set on the old <main> (an opened <details>, a chosen tab) is rebuilt
// from the server's markup unless the script restores it. See
// `.claude/rules/browser-assets.md` §1.
//
// The harness runs one gateway with no peers, so a real peer page cannot be
// reached here; the peer page's own rendering is covered by the Rust tests in
// `server/home_page/peer_detail.rs`. What can run is its not-found page, and
// the same refresh behaviour on /routing, which uses the same tab markup and
// the same script.

const shellUrl = process.env.FREENET_SHELL_URL;
const origin = shellUrl ? new URL(shellUrl).origin : undefined;

test.skip(
  !shellUrl,
  "FREENET_SHELL_URL is not set — run via `cargo test --test playwright_shell`",
);

/// Wait for the next auto-refresh of the current page to land.
async function nextRefresh(page: Page): Promise<void> {
  const path = new URL(page.url()).pathname;
  const response = await page.waitForResponse(
    (r) => new URL(r.url()).pathname === path && r.request().method() === "GET",
    { timeout: 20_000 },
  );
  await response.finished();
  // The swap runs after the body is parsed; give it a frame to apply.
  await page.waitForTimeout(250);
}

test.describe("routing page", () => {
  test("the home dashboard links to /routing, and /routing links back", async ({
    page,
  }) => {
    await page.goto(`${origin}/`, { waitUntil: "domcontentloaded" });
    const link = page.locator('a[href="/routing"]').first();
    await expect(link).toBeVisible();
    await link.click();
    await expect(page).toHaveURL(`${origin}/routing`);
    await expect(page.locator(".header-scope")).toHaveText(/routing/i);
    await expect(page.locator("main .card").first()).toBeVisible();
    await expect(page.locator('meta[http-equiv="refresh"]')).toHaveCount(0);

    await page.locator('main a[href="/"]').click();
    await expect(page).toHaveURL(`${origin}/`);
  });

  test("/routing does not scroll sideways on a phone, diagnostics open or closed", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(`${origin}/routing`, { waitUntil: "domcontentloaded" });
    const overflow = () =>
      page.evaluate(
        () =>
          document.documentElement.scrollWidth -
          document.documentElement.clientWidth,
      );
    expect(
      await overflow(),
      "document scrolls horizontally",
    ).toBeLessThanOrEqual(0);
    const summary = page.locator("details#routing-diagnostics summary");
    if ((await summary.count()) > 0) {
      await summary.click();
      expect(
        await overflow(),
        "document scrolls horizontally with the diagnostics open",
      ).toBeLessThanOrEqual(0);
    }
  });

  test("/routing survives the auto-refresh with what the reader opened", async ({
    page,
  }) => {
    await page.goto(`${origin}/routing`, { waitUntil: "domcontentloaded" });
    const headings = await page
      .locator("main h2, main summary")
      .allTextContents();
    expect(headings.length).toBeGreaterThan(0);

    const details = page.locator("details#routing-diagnostics");
    const hasDetails = (await details.count()) > 0;
    if (hasDetails) {
      await details.locator("summary").click();
      await expect(details).toHaveAttribute("open", "");
    }
    const getTab = page.locator('.tab-label[data-tab="get"]');
    const hasTabs = (await getTab.count()) > 0;
    if (hasTabs) {
      await getTab.click();
      await expect(getTab).toHaveClass(/tab-active/);
    }

    await nextRefresh(page);

    expect(
      await page.locator("main h2, main summary").allTextContents(),
    ).toEqual(headings);
    if (hasDetails) {
      await expect(
        page.locator("details#routing-diagnostics"),
        "an opened <details> must stay open across the <main> swap",
      ).toHaveAttribute("open", "");
    }
    if (hasTabs) {
      await expect(
        page.locator('.tab-label[data-tab="get"]'),
        "the chosen tab must survive the <main> swap",
      ).toHaveClass(/tab-active/);
      await expect(page.locator("#panel-get")).toHaveClass(/tab-panel-active/);
    }
  });
});

test.describe("peer page", () => {
  test("an address that is not a connected peer gets the not-found page", async ({
    page,
  }) => {
    const response = await page.goto(`${origin}/peer/192.0.2.1:9`, {
      waitUntil: "domcontentloaded",
    });
    expect(response!.status()).toBe(200);
    await expect(page.locator("main")).toContainText("Peer Not Found");
    await expect(page.locator('main a[href="/"]')).toBeVisible();
    await expect(page.locator('meta[http-equiv="refresh"]')).toHaveCount(0);
  });
});
