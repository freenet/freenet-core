import { test, expect, type Page } from "@playwright/test";

// Regression tests for the local dashboard's long-table filter
// (`home_page/assets/dashboard.js`).
//
// Why this lives in Playwright and not in Rust. The Rust tests in
// `home_page.rs` assert the emitted MARKUP — that the controls are present,
// labelled, and bound to the right table. They never execute a line of
// `dashboard.js`: no DOM, no scroll position, no focus, no auto-refresh. Every
// defect this feature has actually shipped lived in the JS and was invisible
// to them, and two of the three below were introduced by the FIX for the one
// before it:
//
//   1. the filter box emptied on every 5s auto-refresh, because the
//      post-refresh path restored the sort but not the filter — a filter you
//      could not finish typing into;
//   2. restoring focus to fix (1) reintroduced the caret but scrolled the
//      viewport: `focus()` scrolls the element into view, and the input sits
//      at the TOP of a card whose table can be thousands of pixels tall, so a
//      user who filtered and then scrolled down to READ the matches was yanked
//      back every five seconds. Measured before the fix: scrollY 3000 -> 231.
//      Note `preventScroll` does NOT settle this on its own — WebKit's
//      scroll-into-view is deferred and overrode both it and an explicit
//      scroll reassignment, landing on 1341 every time. The shipped fix
//      restores focus only when the box is already visible, so the two tests
//      below are complementary: off screen means no refocus and no scroll, on
//      screen means focus and caret both come back;
//   3. the filter matched only `textContent`, so pasting a full contract key —
//      what the copy button beside every row puts on the clipboard — matched
//      nothing, because the cell renders a 12-character abbreviation.
//
// `table_filter.test.mjs` covers the pure decision functions under Node. What
// it cannot cover, and what is asserted here, is everything that only exists
// in a real engine: scroll position, focus, the caret, and the `<main>` swap.
//
// Fixture note. The filter controls are rendered unconditionally by
// `cards.rs`, so they are present even on a freshly-started node with no
// peers, and these tests fail loudly rather than skipping if they are missing.
// Where a test needs rows or page height that the test node does not have, it
// builds them in-page: the collapse and filter are entirely client-side and
// operate on whatever is in the DOM, so a cloned row exercises exactly the
// production path. The one thing that must NOT be built inside `<main>` is
// page height, because the refresh replaces `<main>` and the resulting shrink
// clamps the scroll position — see the comment on the spacer below.

const shellUrl = process.env.FREENET_SHELL_URL;

// The dashboard is served from the node's root, on the same origin as the
// shell URL the harness hands us.
const dashboardUrl = shellUrl ? new URL("/", shellUrl).toString() : undefined;

// Height of the scroll spacer used by the viewport test. Far larger than the
// ~50px tolerance below, so a regression that scrolls the filter back into
// view is unmistakable rather than marginal.
const SPACER_PX = 6000;

// Where the viewport test parks the scroll position. Far enough down that the
// filter input is well off-screen (so a scroll-into-view regression must move
// the page a long way), and far enough from either end that ordinary row
// churn cannot clamp it.
const SCROLL_TO_PX = 3000;

// The auto-refresh runs every 5s while the tab is visible. Allow generous
// slack for a contended CI runner without letting a stalled refresh pass as a
// success — the tests assert a refresh ACTUALLY happened before drawing any
// conclusion from it.
const REFRESH_TIMEOUT_MS = 25_000;

test.skip(
  !shellUrl,
  "FREENET_SHELL_URL is not set — run via `cargo test --test playwright_shell`",
);

/** Wait for one auto-refresh, detected by the uptime text changing. */
async function waitForRefresh(page: Page): Promise<boolean> {
  const before = await page.locator(".uptime").textContent();
  return page
    .waitForFunction(
      (prev) => {
        const el = document.querySelector(".uptime");
        return !!el && el.textContent !== prev;
      },
      before,
      { timeout: REFRESH_TIMEOUT_MS },
    )
    .then(
      () => true,
      () => false,
    );
}

test.describe("dashboard long-table filter", () => {
  test("an auto-refresh does not move the viewport while the filter is focused", async ({
    page,
  }) => {
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const filter = page.locator('.table-filter[data-filter-for="peers"]');
    // Fail loudly rather than skipping: the controls are unconditional, so
    // their absence means the dashboard changed shape and this test has
    // stopped covering anything.
    await expect(
      filter,
      "the peers filter controls are rendered unconditionally by cards.rs",
    ).toBeAttached();

    // Make the page tall enough to scroll. The height must come from OUTSIDE
    // `<main>`: the refresh replaces `<main>`'s innerHTML wholesale, so rows
    // cloned into a table vanish on the first refresh, the page shrinks, and
    // the browser CLAMPS scrollY. That clamping looks exactly like the bug
    // under test — an early version of this test failed with 136px of drift
    // for that reason alone. A body-level spacer survives the swap, so any
    // movement observed below is the focus restore and nothing else.
    await page.evaluate((h) => {
      const spacer = document.createElement("div");
      spacer.id = "scroll-spacer";
      spacer.style.height = `${h}px`;
      document.body.appendChild(spacer);
    }, SPACER_PX);

    // Focus the filter, then scroll well away from it — the exact posture of
    // someone who has filtered and is now reading the matches.
    const input = filter.locator(".tf-input");
    /* Order matters, and getting it wrong made this test fail 1 run in 8.
       WebKit scrolls a newly-focused element into view ASYNCHRONOUSLY, after
       the focus() call has returned. Focusing and then immediately scrolling
       means that deferred scroll can land AFTER the baseline is taken, which
       reads as the refresh having moved the page when it did not.

       Polling for "two consecutive equal reads" does not fix it either — the
       poll can observe two equal reads in the window before the deferred
       scroll fires. So: focus FIRST, give the deferred scroll time to happen,
       and only THEN scroll to the offset under test. Nothing is left pending
       by the time the baseline is read. */
    await input.focus();
    await page.waitForTimeout(400);
    await page.evaluate((y) => window.scrollTo(0, y), SCROLL_TO_PX);
    await page.waitForTimeout(150);
    const scrollBefore = await page.evaluate(() => window.scrollY);
    expect(
      scrollBefore,
      "the page must actually be scrolled, or this test cannot observe a jump",
    ).toBeGreaterThan(200);

    expect(
      await waitForRefresh(page),
      "no auto-refresh fired — the assertion below would pass vacuously",
    ).toBe(true);
    // Let the post-swap restore run.
    await page.waitForTimeout(500);

    const scrollAfter = await page.evaluate(() => window.scrollY);
    // A few pixels of drift is possible if the refresh changes row heights;
    // the bug this guards against moved the viewport by thousands.
    expect(
      Math.abs(scrollAfter - scrollBefore),
      `refresh moved the viewport from ${scrollBefore} to ${scrollAfter}`,
    ).toBeLessThan(50);

    // Focus is deliberately NOT restored here, and that is the mechanism the
    // scroll assertion above depends on: the box is off screen, so refocusing
    // it would scroll. The complementary case — box on screen, focus and caret
    // both restored — is the next test, so "stop restoring focus" cannot pass
    // this file as a whole.
    const refocused = await page.evaluate(() => {
      const el = document.activeElement as HTMLElement | null;
      return !!(el && el.classList && el.classList.contains("tf-input"));
    });
    expect(
      refocused,
      "an off-screen filter box must not steal focus back on refresh",
    ).toBe(false);
  });

  test("the filter value and caret survive the auto-refresh", async ({
    page,
  }) => {
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });
    const input = page.locator('.table-filter[data-filter-for="peers"] .tf-input');
    await expect(input).toBeAttached();

    await input.fill("10.9");
    await input.focus();
    await page.evaluate(() => {
      const el = document.activeElement as HTMLInputElement;
      el.setSelectionRange(2, 2);
    });

    /* Pin the precondition. Focus is restored only when the box is ON SCREEN
       (see the sibling test), and the card can drift as the page settles —
       tables gain rows, the ring diagram sizes itself — which pushed the box
       out of view on ~2 runs in 12 and made this test look flaky when the
       product was doing exactly what it should. Scroll it into view and assert
       it is really there, so a failure below means focus was lost rather than
       correctly declined. */
    await input.scrollIntoViewIfNeeded();
    await page.waitForTimeout(150);
    const visibleBefore = await input.evaluate((el) => {
      const r = el.getBoundingClientRect();
      const h = window.innerHeight || document.documentElement.clientHeight;
      return r.bottom > 0 && r.top < h;
    });
    expect(
      visibleBefore,
      "the filter box must be on screen, or declining to refocus is correct",
    ).toBe(true);

    expect(
      await waitForRefresh(page),
      "no auto-refresh fired — the assertions below would pass vacuously",
    ).toBe(true);
    await page.waitForTimeout(500);

    const state = await page.evaluate(() => {
      const el = document.activeElement as HTMLInputElement | null;
      return {
        focused: !!(el && el.classList && el.classList.contains("tf-input")),
        value: el ? el.value : null,
        caret: el ? el.selectionStart : null,
      };
    });
    expect(state.focused).toBe(true);
    expect(state.value).toBe("10.9");
    expect(state.caret).toBe(2);
  });

  test("a full contract key matches the row that displays its abbreviation", async ({
    page,
  }) => {
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const filter = page.locator('.table-filter[data-filter-for="peers"]');
    await expect(filter).toBeAttached();

    // Reproduce how cards.rs renders a contract key: an abbreviation in the
    // cell text, the full value only in data-copy on the copy button.
    const FULL = "ZZTESTKEY1111111111111111111111111111111111";
    await page.evaluate((full) => {
      const tbody = document.querySelector(
        'table[data-table-id="peers"] tbody',
      ) as HTMLElement;
      const proto = tbody.querySelector("tr") as HTMLElement;
      const row = proto.cloneNode(true) as HTMLElement;
      const cell = row.querySelector("td");
      if (cell) {
        cell.textContent = `${full.slice(0, 12)}…`;
        const btn = document.createElement("button");
        btn.className = "copy-key";
        btn.setAttribute("data-copy", full);
        cell.appendChild(btn);
      }
      tbody.appendChild(row);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (window as any).applyAllTableViews();
    }, FULL);

    await filter.locator(".tf-input").fill(FULL);

    const visible = await page.evaluate(() =>
      [
        ...document.querySelectorAll('table[data-table-id="peers"] tbody tr'),
      ].filter((r) => (r as HTMLElement).style.display !== "none").length,
    );
    // Exactly one: the planted row. More would mean the query is matching
    // something invisible; zero is the original bug.
    expect(
      visible,
      "pasting a full key must surface exactly the row that abbreviates it",
    ).toBe(1);
  });
});
