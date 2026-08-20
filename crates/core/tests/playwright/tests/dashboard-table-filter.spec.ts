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
// Fixture note, and a correction. An earlier version of these tests asserted
// that `cards.rs` renders the filter controls unconditionally, and failed
// loudly in CI when it did not: BOTH the peers and the contracts cards
// short-circuit to an empty variant (or to no card at all) when they have no
// rows, and the harness node is a single isolated peer with neither. The
// controls are therefore absent exactly where CI runs.
//
// So these tests build the table and its controls in-page, mirroring
// `table_filter_controls` in `cards.rs`. That is safe against drift only
// because the Rust side pins the same markup: see
// `filter_fixture_markup_matches_table_filter_controls` in `home_page.rs`,
// which fails if the emitted class names or attributes stop matching the
// fixture below. If you change one, that test tells you to change the other.
//
// What is NOT synthetic is the code under test: `dashboard.js` binds through
// delegated listeners on `document` and operates on whatever rows are in the
// DOM, so the fixture drives exactly the production path.
//
// One thing must NOT be built inside `<main>`: page height. The refresh
// replaces `<main>`, so anything added there vanishes and the resulting shrink
// clamps the scroll position — see the comment on the spacer below.

const shellUrl = process.env.FREENET_SHELL_URL;

// The dashboard is served from the node's root, on the same origin as the
// shell URL the harness hands us.
const dashboardUrl = shellUrl ? new URL("/", shellUrl).toString() : undefined;

// The auto-refresh runs every 5s while the tab is visible. Allow generous
// slack for a contended CI runner without letting a stalled refresh pass as a
// success — the tests assert a refresh ACTUALLY happened before drawing any
// conclusion from it.
const REFRESH_TIMEOUT_MS = 25_000;

test.skip(
  !shellUrl,
  "FREENET_SHELL_URL is not set — run via `cargo test --test playwright_shell`",
);

/* Mirrors `table_filter_controls` + a sortable table in `cards.rs`. Pinned by
   `filter_fixture_markup_matches_table_filter_controls` on the Rust side. */
const FIXTURE_TABLE_ID = "fixturepeers";

function fixtureCardHtml(rows: number): string {
  let body = "";
  for (let i = 0; i < rows; i++) {
    body +=
      `<tr><td data-sort="10.9.0.${i}"><code>10.9.0.${i}:31337</code></td>` +
      `<td data-sort="${1000 - i}">${1000 - i}s</td></tr>`;
  }
  return (
    '<div class="card" id="filter-fixture">' +
    '<div class="card-header"><h2>Fixture</h2></div>' +
    `<div class="table-filter" data-filter-for="${FIXTURE_TABLE_ID}">` +
    '<input type="search" class="tf-input" placeholder="Filter fixture" ' +
    'aria-label="Filter fixture" autocomplete="off" spellcheck="false">' +
    '<span class="tf-status" role="status" aria-live="polite"></span>' +
    '<button type="button" class="tf-toggle" hidden aria-expanded="false"></button>' +
    "</div>" +
    `<div class="table-wrap"><table class="sortable" data-table-id="${FIXTURE_TABLE_ID}">` +
    '<thead><tr><th data-sort-type="text">Address</th>' +
    '<th data-sort-type="num">Connected</th></tr></thead>' +
    `<tbody>${body}</tbody></table></div></div>`
  );
}

/* Inject the fixture into EVERY response for the dashboard page, so it is
   present in the refreshed HTML exactly as server-rendered content would be.

   Both simpler approaches are broken, and each hid a different thing:
     - appending to <main> after load: the refresh replaces <main>'s innerHTML
       and the fixture disappears on the first refresh, which is the event
       these tests wait for;
     - appending to <body>: it survives, but so does the INPUT ELEMENT, so
       focus is never lost and the restore path under test never runs. That
       version passed while asserting nothing.

   Rewriting the response is what makes the fixture behave like real markup:
   destroyed by the swap and rebuilt from the new HTML, exactly as the peers
   table is on a node that has peers. */
async function routeFixture(page: Page, rows: number): Promise<void> {
  const html = fixtureCardHtml(rows);
  await page.route(dashboardUrl!, async (route) => {
    const response = await route.fetch();
    const body = await response.text();
    const marker = "</main>";
    const i = body.lastIndexOf(marker);
    if (i < 0) {
      throw new Error(
        "dashboard HTML has no </main> to inject the fixture into",
      );
    }
    await route.fulfill({
      response,
      body: body.slice(0, i) + html + body.slice(i),
    });
  });
}

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
    await routeFixture(page, 150);
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const filter = page.locator(
      `.table-filter[data-filter-for="${FIXTURE_TABLE_ID}"]`,
    );
    await expect(filter).toBeAttached();

    const input = filter.locator(".tf-input");
    /* Order matters, and getting it wrong made this test fail 1 run in 8.
       WebKit scrolls a newly-focused element into view ASYNCHRONOUSLY, after
       focus() has returned, so focusing and then immediately scrolling lets
       that deferred scroll land AFTER the baseline is taken — which reads as
       the refresh having moved the page when it did not. Focus first, let the
       deferred scroll happen, and only then scroll to the offset under test. */
    await input.focus();
    await page.waitForTimeout(400);
    /* Scroll relative to the BOX, not to a fixed offset. The fixture is
       appended after <main>, whose height varies with how much the node has to
       report, so a hardcoded offset can land right on the filter box and leave
       it visible — in which case focus IS restored, correctly, and the test
       fails for the wrong reason. Put the box a full viewport above the top
       edge so it is unambiguously off screen. */
    await input.evaluate((el) => {
      const y = window.scrollY + el.getBoundingClientRect().top;
      window.scrollTo(0, y + window.innerHeight + 600);
    });
    await page.waitForTimeout(150);

    const boxOffScreen = await input.evaluate(
      (el) => el.getBoundingClientRect().bottom < 0,
    );
    expect(
      boxOffScreen,
      "the filter box must be off screen, or declining to refocus is wrong",
    ).toBe(true);

    const scrollBefore = await page.evaluate(() => window.scrollY);
    expect(
      scrollBefore,
      "the page must actually be scrolled, or this test cannot observe a jump",
    ).toBeGreaterThan(200);

    /* Measure a ROW's position within the viewport, not window.scrollY.
       Raw scrollY is the wrong instrument here: <main> changes height on every
       refresh (uptime, counters, row counts), and browsers apply SCROLL
       ANCHORING to compensate — adjusting scrollY precisely so that what the
       user is looking at stays put. Asserting on scrollY therefore reports an
       88px "jump" for a viewport that did not visually move at all. What the
       user actually cares about, and what this test should pin, is that the
       row under their eyes stays under their eyes. */
    const rowTopBefore = await page.evaluate(
      (id) =>
        document
          .querySelector(`table[data-table-id="${id}"] tbody tr:nth-child(100)`)!
          .getBoundingClientRect().top,
      FIXTURE_TABLE_ID,
    );

    expect(
      await waitForRefresh(page),
      "no auto-refresh fired — the assertion below would pass vacuously",
    ).toBe(true);
    await page.waitForTimeout(500);

    const rowTopAfter = await page.evaluate(
      (id) =>
        document
          .querySelector(`table[data-table-id="${id}"] tbody tr:nth-child(100)`)!
          .getBoundingClientRect().top,
      FIXTURE_TABLE_ID,
    );
    expect(
      Math.abs(rowTopAfter - rowTopBefore),
      `the row under the reader moved from ${Math.round(rowTopBefore)} to ${Math.round(rowTopAfter)} in the viewport`,
    ).toBeLessThan(50);

    // Focus is deliberately NOT restored here, and that is the mechanism the
    // assertion above depends on: the box is off screen, so refocusing it
    // would scroll. The complementary case — box on screen, focus and caret
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
    await routeFixture(page, 40);
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const input = page.locator(
      `.table-filter[data-filter-for="${FIXTURE_TABLE_ID}"] .tf-input`,
    );
    await expect(input).toBeAttached();

    await input.fill("10.9");
    await input.focus();
    await page.evaluate(() => {
      const el = document.activeElement as HTMLInputElement;
      el.setSelectionRange(2, 2);
    });

    /* Pin the precondition. Focus is restored only when the box is ON SCREEN
       (see the sibling test), and the page can drift as it settles, which
       pushed the box out of view on ~2 runs in 12 and made this look flaky
       when the product was doing exactly the right thing. */
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
    await routeFixture(page, 30);
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const filter = page.locator(
      `.table-filter[data-filter-for="${FIXTURE_TABLE_ID}"]`,
    );
    await expect(filter).toBeAttached();

    // Reproduce how cards.rs renders a contract key: an abbreviation in the
    // cell text, the full value only in data-copy on the copy button.
    const FULL = "ZZTESTKEY1111111111111111111111111111111111";
    await page.evaluate(
      ({ id, full }) => {
        const tbody = document.querySelector(
          `table[data-table-id="${id}"] tbody`,
        ) as HTMLElement;
        const row = document.createElement("tr");
        row.innerHTML =
          "<td>" +
          full.slice(0, 12) +
          '…<button type="button" class="copy-key" data-copy="' +
          full +
          '"></button></td><td data-sort="1">1s</td>';
        tbody.appendChild(row);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (window as any).applyAllTableViews();
      },
      { id: FIXTURE_TABLE_ID, full: FULL },
    );

    await filter.locator(".tf-input").fill(FULL);

    const visible = await page.evaluate(
      (id) =>
        [
          ...document.querySelectorAll(`table[data-table-id="${id}"] tbody tr`),
        ].filter((r) => (r as HTMLElement).style.display !== "none").length,
      FIXTURE_TABLE_ID,
    );
    // Exactly one: the planted row. More would mean the query is matching
    // something invisible; zero is the original bug.
    expect(
      visible,
      "pasting a full key must surface exactly the row that abbreviates it",
    ).toBe(1);
  });
});
