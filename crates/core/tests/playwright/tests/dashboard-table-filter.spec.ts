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
// The page must be TALL, or the offsets these tests scroll to are clamped and
// every measurement below is taken against a page that never went where it was
// asked to. The fixture supplies that height itself (see the spacer in
// `fixtureCardHtml`) rather than relying on what the node happens to be
// reporting. It does NOT need to survive the `<main>` swap: see the note in
// the first test for what was actually measured across that swap.

const shellUrl = process.env.FREENET_SHELL_URL;

// The dashboard is served from the node's root, on the same origin as the
// shell URL the harness hands us.
const dashboardUrl = shellUrl ? new URL("/", shellUrl).toString() : undefined;

// The auto-refresh runs every 5s while the tab is visible. Allow generous
// slack for a contended CI runner without letting a stalled refresh pass as a
// success — the tests assert a refresh ACTUALLY happened before drawing any
// conclusion from it.
const REFRESH_TIMEOUT_MS = 25_000;

/* Attribute stamped on the fixture's status element before a refresh, so the
   wait for that refresh can require a REBUILT one rather than accepting the
   one already on the page. See waitForRefresh. */
const PRE_REFRESH_MARK = "data-pre-refresh";

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
    `<tbody>${body}</tbody></table></div>` +
    /* A spacer with a FIXED height, inside the fixture so it is re-injected on
       every refresh and its height never changes.

       The row count cannot supply this: the collapse hides all but 25 rows, so
       150 rows render as ~25 rows tall. On a CI node with no peers and no
       contracts the rest of <main> is short too, so the page was barely taller
       than the viewport and the offsets these tests scroll to were clamped —
       `window.scrollTo` is a request, and a short page grants only part of it.
       Locally it passed only because that node has 210 peers and supplied the
       height by accident. This makes the height a property of the fixture
       rather than of whatever the node happens to be doing. */
    '<div style="height:4000px" aria-hidden="true"></div>' +
    "</div>"
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
    /* Inject at the TOP of <main>, not the bottom. Everything the node
       reports — uptime, counters, row counts — changes height between
       refreshes, so a fixture placed after it SHIFTS by that delta and its
       rows change viewport position without the viewport having moved at all.
       Measured 68-121px of drift from this alone, which is indistinguishable
       from a small real jump. Placing the fixture first removes the confound
       instead of trying to tolerate it. */
    /* Search AFTER <body>. The inline <script> in <head> is `dashboard.js`,
       whose comments discuss the `<main>` swap in prose — so a naive
       indexOf("<main") finds a COMMENT and injects the fixture into the middle
       of the script, breaking every selector on the page with an error that
       says only "element not found". */
    const bodyStart = body.indexOf("<body");
    const open = bodyStart < 0 ? -1 : body.indexOf("<main", bodyStart);
    const openEnd = open < 0 ? -1 : body.indexOf(">", open);
    if (openEnd < 0) {
      throw new Error("dashboard HTML has no <main> to inject the fixture into");
    }
    const at = openEnd + 1;
    let out = body.slice(0, at) + html + body.slice(at);
    /* Mutation hook, for checking that these assertions can actually go red.
       `dashboard.js` is inlined into this response, so setting
       FREENET_DASHBOARD_MUTATION to a `from=>to` pair serves a deliberately
       broken copy of it without rebuilding the node. Unset in CI and in any
       normal run; it throws rather than silently doing nothing if the text it
       is told to replace is not there, because a mutation that does not apply
       reads exactly like an assertion that cannot fail. See the mutation
       results recorded above the viewport assertion in the first test. */
    const mutation = process.env.FREENET_DASHBOARD_MUTATION;
    if (mutation) {
      /* Split on the FIRST "=>" only, and require it: `to` is very often an
         arrow function, and a plain `split("=>")` would truncate it at the
         arrow. A missing separator would silently mean "delete", which the
         did-it-apply guard below cannot catch because a deletion does change
         the string. */
      const sep = mutation.indexOf("=>");
      if (sep < 0) {
        throw new Error(
          'FREENET_DASHBOARD_MUTATION must be "from=>to" — no "=>" found, and ' +
            "a bare `from` would silently delete rather than replace",
        );
      }
      const from = mutation.slice(0, sep);
      const to = mutation.slice(sep + 2);
      if (!from) {
        throw new Error(
          "FREENET_DASHBOARD_MUTATION has an empty `from`, which prepends " +
            "rather than replaces",
        );
      }
      /* A FUNCTION replacement, so `$&`, `$1` and friends in `to` are literal
         text rather than String.replace substitution patterns. */
      const mutated = out.replace(from, () => to);
      if (mutated === out) {
        throw new Error(
          `FREENET_DASHBOARD_MUTATION found no "${from}" to replace — the ` +
            "mutation did not apply, so a green run proves nothing",
        );
      }
      out = mutated;
    }
    await route.fulfill({ response, body: out });
  });
}

/* Viewport-relative top of a FIXED row, chosen so it is the same element
 * before and after the refresh.
 *
 * Row 20 sits inside the 25-row collapse cap, so it is rendered in both
 * states. An earlier version anchored on "the last visible row", which is not
 * a stable identity: the collapse is re-applied AFTER the `<main>` swap, so
 * between the swap and that call every row is briefly visible and the last one
 * is row 150 rather than row 25. Measuring that reported a large jump for a
 * viewport that never moved.
 *
 * The helper asserts the row is neither hidden nor zero-height, because a
 * `display: none` row reports `top: 0, height: 0` and would silently compare
 * 0 with 0 — a vacuous shape this file has already fallen into once. */
/* Must stay inside `COLLAPSE_ROWS` in dashboard.js (25 at time of writing).
   The coupling is across files and cannot be imported, so it is stated here:
   if that cap is ever lowered to 20 or below, this anchor falls outside it.
   That fails loudly rather than silently — `anchorRowTop` asserts the row is
   not hidden — but the failure would point here rather than at the cap, so
   the next person to change the cap should read this line. */
const ANCHOR_ROW = 20;

async function anchorRowTop(page: Page): Promise<number> {
  const r = await page.evaluate(
    ({ id, nth }) => {
      const row = document.querySelector(
        `table[data-table-id="${id}"] tbody tr:nth-child(${nth})`,
      ) as HTMLElement | null;
      if (!row) return null;
      const rect = row.getBoundingClientRect();
      return {
        top: rect.top,
        height: rect.height,
        hidden: row.style.display === "none",
      };
    },
    { id: FIXTURE_TABLE_ID, nth: ANCHOR_ROW },
  );

  expect(r, `no row ${ANCHOR_ROW} to anchor the viewport assertion on`).not.toBeNull();
  expect(
    r!.hidden,
    "the anchor row is hidden by the collapse — it must sit inside the cap",
  ).toBe(false);
  expect(
    r!.height,
    "the anchor row has zero height, so measuring its position proves nothing",
  ).toBeGreaterThan(0);
  return r!.top;
}

/* Wait for `count` RENDERING FRAMES.
 *
 * Use this, never a millisecond sleep, whenever the test needs an
 * engine-deferred scroll to have landed. That distinction is the whole of
 * #5390 and is worth stating precisely, because a sleep looks like it does the
 * same job and does not.
 *
 * WebKit reveals the caret of a newly-focused text field on a later RENDERING
 * FRAME, not synchronously inside `focus()`. On an idle machine the next frame
 * is ~16ms away, so any sleep at all covers it and the reveal lands while the
 * box is still where it was focused — a no-op. On a loaded CI runner frames are
 * produced far more slowly: measured here, with the machine running 24 busy
 * loops, `requestAnimationFrame` fired 15 times in 12.3 SECONDS, about 0.8s
 * apart. A 400ms sleep then spans zero frames or one, so the reveal is
 * routinely still pending when the test scrolls the box off screen, and it
 * lands on the next frame — measured 1.7s later — dragging the page back to
 * the caret with no refresh anywhere near it.
 *
 * The test then measured that movement and blamed the code under test. The
 * giveaway is that the reported jump was always exactly
 * `scrollY_before - (boxTop + 6)`, a constant of the SETUP and not of anything
 * the refresh did: two CI failures a day apart, on different branches, both
 * reported a delta of exactly 1314px. See the note above the swap assertion in
 * the first test for what was measured across the swap itself.
 *
 * A frame count is the barrier that corresponds to the event; a duration is a
 * guess about how long a frame takes on a machine you are not running on.
 *
 * TWO frames by default, and the two is load-bearing rather than round: a rAF
 * callback runs at the START of a rendering update, before layout, so a reveal
 * flushed during frame N is only observable to a callback registered for frame
 * N+1. One frame does not prove the reveal has been through. Do not "simplify"
 * this to 1 — nothing would fail, until a loaded runner made it matter.
 *
 * The bail inside is not decoration. `page.evaluate` carries no timeout of its
 * own, so a page that stops producing frames would otherwise hang until the
 * 60s test timeout and report "Test timeout exceeded" — the generic message
 * this file exists to stop handing the next person. Every frame wait in this
 * file goes through here for that reason; do not open-code
 * `requestAnimationFrame` in another evaluate. */
async function waitForFrames(page: Page, count = 2): Promise<void> {
  await page.evaluate(async (n) => {
    for (let i = 0; i < n; i++) {
      await new Promise<void>((resolve, reject) => {
        const bail = setTimeout(
          () =>
            reject(
              new Error(
                "no rendering frame arrived within 15s — the page is not being " +
                  "rendered, so nothing here can wait for an engine-deferred scroll",
              ),
            ),
          15_000,
        );
        requestAnimationFrame(() => {
          clearTimeout(bail);
          resolve();
        });
      });
    }
  }, count);
}

/* Where to park the filter box relative to the viewport. */
type ParkPosition = "above the viewport" | "straddling the top edge";

/* Scroll the filter box to `where`, and return only once the ENGINE has
 * stopped moving the page.
 *
 * Scrolling is a request, not a result: WebKit can still have a deferred caret
 * reveal queued (see waitForFrames), and it will undo the scroll on whatever
 * frame it happens to run on. Rather than assume that cannot happen, park the
 * box and then WATCH: hold the position across three rendering frames, and if
 * the engine moves it, re-park from the box's new position and watch again.
 * The reveal is one-shot, so this converges — it does not paper over a page
 * that keeps moving, it just refuses to hand a moving page to an assertion.
 *
 * The convergence and the hold loop are both MEASURED, not assumed, because an
 * unexercised recovery path is not a recovery path. Deleting the
 * `waitForFrames` above the first call — restoring the exact race #5390 was
 * about — makes this report `attempts: 2` on every one of six loaded runs and
 * the test still pass: the reveal fires during the first hold, the re-park
 * sticks, and it never needs a third. With that barrier back in place it is
 * usually 1 and occasionally still 2, so the loop is live code rather than
 * decoration even in the shipped configuration.
 *
 * `settled: false` means the page never held still, which is a real failure of
 * the precondition and must be surfaced as one rather than measured. `found:
 * false` is the DIFFERENT failure of the box not being in the page at all —
 * kept separate so a vanished fixture is not reported as a restless engine.
 *
 * Note what "settled" does NOT promise: `scrollY` is read back AFTER the
 * scroll, so a page too short to grant the offset settles happily at whatever
 * it was clamped to. The callers check where the box actually ended up. */
async function parkFilterBox(
  page: Page,
  id: string,
  where: ParkPosition,
): Promise<{
  settled: boolean;
  found: boolean;
  scrollY: number;
  attempts: number;
}> {
  const HOLD_FRAMES = 3;
  const MAX_ATTEMPTS = 8;
  let scrollY = 0;
  /* The frame waits are driven from HERE rather than inside one long
     `page.evaluate`, so every one of them goes through waitForFrames and
     inherits its bail. An evaluate that awaits 24 open-coded frames is the one
     place in this file that could hang with no explanation. */
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const landed = await page.evaluate(
      ({ id, where }) => {
        const el = document.querySelector(
          `.table-filter[data-filter-for="${id}"] .tf-input`,
        ) as HTMLElement | null;
        if (!el) return null;
        const r = el.getBoundingClientRect();
        const boxTop = window.scrollY + r.top;
        window.scrollTo(
          0,
          where === "above the viewport"
            ? /* A full viewport clear of the top edge, so the box is
                 unambiguously off screen however the page is laid out. */
              boxTop + window.innerHeight + 600
            : /* Lower half on screen, upper half above it. */
              boxTop + r.height / 2,
        );
        return window.scrollY;
      },
      { id, where },
    );
    if (landed === null) {
      return { settled: false, found: false, scrollY, attempts: attempt };
    }
    scrollY = landed;
    let held = 0;
    for (; held < HOLD_FRAMES; held++) {
      await waitForFrames(page, 1);
      const now = await page.evaluate(() => window.scrollY);
      if (now !== landed) {
        scrollY = now;
        break;
      }
    }
    if (held === HOLD_FRAMES) {
      return { settled: true, found: true, scrollY: landed, attempts: attempt };
    }
  }
  return { settled: false, found: true, scrollY, attempts: MAX_ATTEMPTS };
}

/* Wait for one auto-refresh, and for it to have SETTLED.
 *
 * Detected by the fixture's own `.tf-status` element being REBUILT and then
 * refilled, not by the uptime text changing. Both halves are load-bearing:
 *
 *   - "rebuilt" is the swap. The element is stamped with PRE_REFRESH_MARK
 *     before the wait; the refresh re-parses the fetched HTML, which never
 *     carries that attribute, so an unmarked status can only be a new node.
 *     This replaces watching `.uptime`, which was a proxy for the swap rather
 *     than the swap, and a bad one: the node prints its uptime at MINUTE
 *     granularity once it has been up an hour, so on any node older than that
 *     the text stops changing every 5s and all three refresh tests fail with
 *     "no auto-refresh fired". On CI the harness node is seconds old so it
 *     never bit there — it bites exactly the person trying to reproduce a CI
 *     failure locally against a long-running node, which is the one situation
 *     this file most needs to support.
 *
 *   - "refilled" is the restore. `restoreTableFilters` runs after the swap,
 *     and until it does the table is in its raw server-rendered state: every
 *     row visible, so thousands of pixels taller than it is about to be, and
 *     the filter box empty. Anything measured in that window compares two
 *     different layouts. The fixture ships `.tf-status` EMPTY and
 *     `applyTableView` is what fills it in, so non-empty means the restore
 *     ran — and no test asserts anything about that text, so waiting on it
 *     cannot make an assertion circular.
 *
 * Then let a frame or two pass, so a scroll the engine deferred across the
 * swap has landed and is caught by the assertion that follows rather than
 * missed by measuring too early.
 *
 * The outcome is a string rather than a boolean so a failure says WHICH half
 * did not happen. "no-refresh" and "no-restore" are very different bugs. */
type RefreshOutcome =
  | "refreshed"
  | "no-fixture"
  | "no-refresh"
  | "no-restore";

async function waitForRefresh(page: Page): Promise<RefreshOutcome> {
  const marked = await page.evaluate(
    ({ id, mark }) => {
      const status = document.querySelector(
        `.table-filter[data-filter-for="${id}"] .tf-status`,
      );
      if (!status) return false;
      status.setAttribute(mark, "1");
      return true;
    },
    { id: FIXTURE_TABLE_ID, mark: PRE_REFRESH_MARK },
  );
  if (!marked) return "no-fixture";

  const settled = await page
    .waitForFunction(
      ({ id, mark }) => {
        const status = document.querySelector(
          `.table-filter[data-filter-for="${id}"] .tf-status`,
        );
        return (
          !!status &&
          !status.hasAttribute(mark) &&
          (status.textContent || "").trim().length > 0
        );
      },
      { id: FIXTURE_TABLE_ID, mark: PRE_REFRESH_MARK },
      { timeout: REFRESH_TIMEOUT_MS },
    )
    .then(
      () => true,
      () => false,
    );
  if (settled) {
    await waitForFrames(page);
    return "refreshed";
  }

  /* Say which half is missing rather than reporting one for the other. */
  return page.evaluate(
    ({ id, mark }) => {
      const status = document.querySelector(
        `.table-filter[data-filter-for="${id}"] .tf-status`,
      );
      if (!status) return "no-fixture" as const;
      return status.hasAttribute(mark)
        ? ("no-refresh" as const)
        : ("no-restore" as const);
    },
    { id: FIXTURE_TABLE_ID, mark: PRE_REFRESH_MARK },
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

    /* A body-level 5000px spacer used to live here, to keep the document tall
       "THROUGHOUT the swap" because `main.innerHTML = ...` supposedly emptied
       <main> for an instant and let WebKit clamp the scroll to ~0. It is gone,
       and the reason is worth recording so it is not re-added: that mechanism
       cannot happen. `innerHTML =` replaces the subtree inside one task with
       no intervening layout, so there is no observable moment at which the
       document is short. Instrumented across the real swap on WebKit — a probe
       wrapped around the assignment itself — the document went 11645 ->
       15916 -> 11645. It GROWS: the fresh HTML arrives with all 150 rows
       showing, and comes back down only when the collapse is re-applied. At no
       point is it short, and `window.scrollY` is unchanged across the whole
       sequence. The spacer was
       protecting against a collapse that never occurred, which is exactly why
       it did not stop the failure it was added for (#5390). */
    const input = filter.locator(".tf-input");
    /* Order matters, and getting it wrong made this test fail 1 run in 8 —
       then 18 runs in 20 once the machine was loaded enough to expose it
       properly (#5390). WebKit scrolls a newly-focused element into view on a
       later rendering FRAME, after focus() has returned, so focusing and then
       scrolling elsewhere lets that deferred scroll land AFTER the baseline is
       taken, which reads as the refresh having moved the page when it did not.
       Focus first, let a frame go by so the reveal resolves against a box that
       is still on screen (a no-op), and only then scroll to the offset under
       test. Frames, not milliseconds: see waitForFrames. */
    await input.focus();
    await waitForFrames(page);
    /* Scroll relative to the BOX, not to a fixed offset. The fixture is
       injected at the top of <main>, whose height varies with how much the
       node has to report, so a hardcoded offset can land right on the filter
       box and leave it visible — in which case focus IS restored, correctly,
       and the test fails for the wrong reason.

       parkFilterBox also refuses to return until the page has HELD the
       position across several frames, so a late engine scroll is caught here,
       as the precondition failure it is, instead of downstream as a phantom
       viewport jump. */
    const parked = await parkFilterBox(
      page,
      FIXTURE_TABLE_ID,
      "above the viewport",
    );
    expect(
      parked.found,
      "the fixture's filter box was not in the page to scroll to",
    ).toBe(true);
    expect(
      parked.settled,
      "the page would not hold still after being scrolled — the engine kept " +
        `moving it (${parked.attempts} attempts, last at ${Math.round(parked.scrollY)})`,
    ).toBe(true);

    /* A single read, deliberately, where this used to be a 5s poll. Once
       parkFilterBox has held the position for three frames the page is not
       expected to move again, so a box that is NOT off screen here is a real
       failure and should be loud rather than waited out. The tolerance for a
       late engine scroll is now stated (2 frames after focus, 3 more after the
       park) instead of being an unbounded poll that hid how much slack there
       actually was. */
    const boxOffScreen = await page.evaluate(
      (id) => {
        const el = document.querySelector(
          `.table-filter[data-filter-for="${id}"] .tf-input`,
        );
        return !!el && el.getBoundingClientRect().bottom < 0;
      },
      FIXTURE_TABLE_ID,
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

    /* Measuring the anchor row's viewport position across the refresh has a
     * long history of false diagnoses, listed here so none of them is
     * proposed again: a hidden-row anchor (comparing 0 with 0), a page-height
     * collapse, a WebKit scroll clamp, an anchor whose identity changed
     * mid-test, and an assertion matching ANY filter box rather than the
     * fixture's own. Each produced a change that stands on its own merits and
     * none of them was the failure — and the second and third were not even
     * real: the document never collapses across the swap (measured; see the
     * spacer note above) and so nothing was ever clamped.
     *
     * The cause, finally, was the test's own setup: WebKit's deferred caret
     * reveal landing after the baseline was taken. That is fixed at its
     * source now (waitForFrames, parkFilterBox) rather than tolerated here, so
     * the measurement below is once again about the refresh and nothing else.
     *
     * The product rule this all protects is one line: do not restore focus to
     * a filter box that is off screen. `focus()` scrolls the element into
     * view, which is the whole mechanism. */
    /* TWO assertions, guarding TWO different things.
     *
     * The viewport measurement below is the END-TO-END statement: whatever the
     * refresh does internally, the page under the reader must not move. The
     * refocus assertion further down is the RULE that the current
     * implementation keeps in order to satisfy it: do not restore focus to a
     * box that is off screen. Keep both — the rule is what a reader can act
     * on, the measurement is what stays true if the implementation changes.
     *
     * What each one actually detects, mutation-tested rather than assumed
     * (#5390). Reproduce with FREENET_DASHBOARD_MUTATION="from=>to" (see
     * routeFixture), which serves a broken `dashboard.js` with no rebuild:
     *
     *   - "restoreTableFilters(focusBeforeSwap);=>restoreTableFilters(focusBeforeSwap);
     *     window.scrollTo(0, 0);" — a refresh that really does move the page.
     *     Fails the measurement below at ~1446px in all three engines, so it
     *     is a live assertion and not a vacuous one.
     *   - "&& focusState.visible)=>)" — the visibility gate removed, focus
     *     restored unconditionally. Fails this test and the straddle test in
     *     all three engines. Chromium and Firefox fail on the named refocus
     *     assertion. WebKit fails on the measurement below, at 1314px, or —
     *     if the refocus lands before the off-screen check is read — on that
     *     precondition instead; both were observed. Note that 1314 figure: a
     *     real regression here produces the same magnitude as the setup
     *     artifact did, so the NUMBER never distinguished them. Only when the
     *     movement happens does.
     *   - "active.blur();=>0;" — the pre-swap blur removed. Fails NOTHING, in
     *     any of the three engines. That contradicts what this comment used to
     *     claim, so the correction is recorded rather than quietly dropped:
     *     the claim was that CI failing at `-599 -> 715` proved it caught
     *     a missing blur. It did not. That delta was this test's own setup
     *     racing WebKit's deferred caret reveal (see waitForFrames), which is
     *     why it reproduced identically AFTER the blur landed, and why it is
     *     always exactly `scrollY_before - (boxTop + 6)` rather than anything
     *     to do with a refresh. The blur is left in place — the swap destroys
     *     the focused input anyway, so it is harmless — but nothing here
     *     guards it, and the next person should not believe otherwise. */
    const rowTopBefore = await anchorRowTop(page);

    expect(
      await waitForRefresh(page),
      "the refresh did not complete: \"no-refresh\" means <main> was never " +
        "swapped, \"no-restore\" means it was but restoreTableFilters did " +
        "not run, \"no-fixture\" means the route intercept stopped " +
        "matching. The assertions below would be measuring the wrong " +
        "page in every case",
    ).toBe("refreshed");

    const rowTopAfter = await anchorRowTop(page);
    expect(
      Math.abs(rowTopAfter - rowTopBefore),
      `the row under the reader moved from ${Math.round(rowTopBefore)} to ${Math.round(rowTopAfter)} in the viewport`,
    ).toBeLessThan(50);

    /* The fixture must have SURVIVED the refresh, i.e. the route intercept
       matched the refresh's own request. `dashboard.js` refreshes with
       `fetch(window.location.href)`, which is the URL routed above — but if
       that ever diverges (a cache-buster query string, say), the fixture would
       be absent from the refreshed HTML and this test would be examining an
       empty page. Assert it explicitly rather than relying on the row lookup
       below happening to throw. */
    const fixtureSurvived = await page.evaluate(
      (id) => !!document.querySelector(`table[data-table-id="${id}"] tbody tr`),
      FIXTURE_TABLE_ID,
    );
    expect(
      fixtureSurvived,
      "the fixture vanished on refresh — the route intercept no longer matches \
       the refresh request, so this test is examining an empty page",
    ).toBe(true);

    // THE assertion of this test. The box is off screen, so restoring focus
    // to it would scroll the viewport away from what the reader is looking at
    // — that is the bug, and this is the decision that causes it. The
    // complementary case (box on screen, focus and caret both restored) is a
    // later test, so "stop restoring focus" cannot satisfy this file.
    /* Ask whether THE FIXTURE'S box was refocused, not whether any filter box
       was. The dashboard renders its own filter controls for the peers and
       contracts cards, so `classList.contains("tf-input")` is true whenever
       any of them holds focus — including one near the top of the page that
       is legitimately visible and legitimately refocused. That is the
       difference between asking about the element under test and asking about
       the page, and it is why this assertion failed on nodes that render
       those cards while passing on ones that do not. */
    const refocused = await page.evaluate((id) => {
      const el = document.activeElement as HTMLElement | null;
      if (!el || !el.classList || !el.classList.contains("tf-input")) return false;
      const wrap = el.closest(".table-filter");
      return !!wrap && wrap.getAttribute("data-filter-for") === id;
    }, FIXTURE_TABLE_ID);
    expect(
      refocused,
      "an off-screen filter box must not steal focus back on refresh",
    ).toBe(false);
  });

  /* The band between "fully visible" and "fully off screen".
   *
   * Both reviewers flagged this independently and they were right: an
   * INTERSECTION test would call a half-visible box visible, let the focus
   * through, and the browser would scroll it fully into view — WebKit ignores
   * `preventScroll`, so nothing stops it. The other two tests cannot see this:
   * one drives the box fully off screen, the other calls
   * `scrollIntoViewIfNeeded()` and so drives it fully ON screen. Neither ever
   * lands in the band where the two definitions disagree, which is exactly
   * where the bug lived. */
  test("a half-visible filter box does not pull the viewport on refresh", async ({
    page,
  }) => {
    await routeFixture(page, 150);
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const input = page.locator(
      `.table-filter[data-filter-for="${FIXTURE_TABLE_ID}"] .tf-input`,
    );
    await expect(input).toBeAttached();
    await input.focus();
    /* Frames, not milliseconds — same reason as the sibling test, and the same
       failure if you get it wrong: the deferred caret reveal lands late under
       load and pulls the box fully into view, so the straddle precondition
       below fails for a reason that has nothing to do with the refresh. */
    await waitForFrames(page);

    // Park the box straddling the TOP edge: its lower half on screen, its
    // upper half above it, and hold it there across several frames.
    const parked = await parkFilterBox(
      page,
      FIXTURE_TABLE_ID,
      "straddling the top edge",
    );
    expect(
      parked.found,
      "the fixture's filter box was not in the page to scroll to",
    ).toBe(true);
    expect(
      parked.settled,
      "the page would not hold still after being scrolled — the engine kept " +
        `moving it (${parked.attempts} attempts, last at ${Math.round(parked.scrollY)})`,
    ).toBe(true);

    /* Query FRESH rather than measuring through the locator resolved earlier:
       a refresh may have replaced the input between the two, and a detached
       element reports `top: 0, bottom: 0` — which reads as "not straddling"
       and points the failure at the wrong thing. Observed exactly that while
       mutation-testing this file. */
    const straddles = await page.evaluate((id) => {
      const el = document.querySelector(
        `.table-filter[data-filter-for="${id}"] .tf-input`,
      );
      if (!el) return null;
      const r = el.getBoundingClientRect();
      const h = window.innerHeight || document.documentElement.clientHeight;
      return { top: r.top, bottom: r.bottom, h, partial: r.top < 0 && r.bottom > 0 };
    }, FIXTURE_TABLE_ID);
    expect(straddles, "the fixture's filter box is not in the page").not.toBeNull();
    expect(
      straddles!.partial,
      `the box must straddle the viewport edge for this test to mean anything \
       (top ${Math.round(straddles!.top)}, bottom ${Math.round(straddles!.bottom)})`,
    ).toBe(true);

    /* Assert the DECISION, as the sibling test does. `isInViewport` requires
       full CONTAINMENT, so a box straddling the edge is not "visible" and
       focus must not be restored to it.
     *
     * The earlier version measured the resulting scroll instead, with a 4px
     * threshold, because the movement here is bounded by the hidden sliver of
     * a ~28px input — 8px in WebKit, 14px in Chromium. That worked but was
     * needlessly delicate: it needed a threshold tight enough to catch 8px
     * while tolerating layout noise, on a page whose height and anchoring
     * behaviour vary by engine and by what the node happens to be serving.
     * The rule it exists to protect is binary, so test it as binary. */
    expect(
      await waitForRefresh(page),
      "the refresh did not complete: \"no-refresh\" means <main> was never " +
        "swapped, \"no-restore\" means it was but restoreTableFilters did " +
        "not run, \"no-fixture\" means the route intercept stopped " +
        "matching. The assertions below would be measuring the wrong " +
        "page in every case",
    ).toBe("refreshed");

    /* Ask whether THE FIXTURE'S box was refocused, not whether any filter box
       was. The dashboard renders its own filter controls for the peers and
       contracts cards, so `classList.contains("tf-input")` is true whenever
       any of them holds focus — including one near the top of the page that
       is legitimately visible and legitimately refocused. That is the
       difference between asking about the element under test and asking about
       the page, and it is why this assertion failed on nodes that render
       those cards while passing on ones that do not. */
    const refocused = await page.evaluate((id) => {
      const el = document.activeElement as HTMLElement | null;
      if (!el || !el.classList || !el.classList.contains("tf-input")) return false;
      const wrap = el.closest(".table-filter");
      return !!wrap && wrap.getAttribute("data-filter-for") === id;
    }, FIXTURE_TABLE_ID);
    expect(
      refocused,
      "a filter box straddling the viewport edge is not fully visible, so " +
        "focusing it would scroll it the rest of the way in — the partial " +
        "band an intersection test would wrongly admit",
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

    /* Focus, caret and scroll-into-view in ONE task, on a freshly queried
       element.

       The refresh destroys this input every five seconds, so a sequence of
       separate Playwright calls has a window between each pair in which the
       element it resolved no longer exists. Under load that window is wide
       enough to hit: measured as
       `locator.scrollIntoViewIfNeeded: Element is not attached to the DOM`,
       and `document.activeElement` is `<body>` in the same situation, which
       would have thrown on setSelectionRange. Doing all three in one evaluate
       removes the window rather than widening the wait around it. */
    const prepared = await page.evaluate((id) => {
      const el = document.querySelector(
        `.table-filter[data-filter-for="${id}"] .tf-input`,
      ) as HTMLInputElement | null;
      if (!el) return false;
      el.focus();
      el.setSelectionRange(2, 2);
      /* Pin the precondition. Focus is restored only when the box is ON SCREEN
         (see the sibling test), and the page can drift as it settles, which
         pushed the box out of view on ~2 runs in 12 and made this look flaky
         when the product was doing exactly the right thing. */
      el.scrollIntoView({ block: "center" });
      return true;
    }, FIXTURE_TABLE_ID);
    expect(
      prepared,
      "the fixture's filter box was not in the page to focus",
    ).toBe(true);
    /* Frames, not milliseconds: WebKit's caret reveal is deferred to a
       rendering frame, and on a loaded machine a frame can be a second away.
       See waitForFrames. */
    await waitForFrames(page);
    /* Assert the SAME predicate the product uses — full containment with a
       pixel of slack, not mere intersection. Checking intersection here while
       the product checks containment is how this test started failing for a
       reason that had nothing to do with the behaviour under test. */
    const visibleBefore = await input.evaluate((el) => {
      const r = el.getBoundingClientRect();
      const h = window.innerHeight || document.documentElement.clientHeight;
      const w = window.innerWidth || document.documentElement.clientWidth;
      return r.top >= -1 && r.left >= -1 && r.bottom <= h + 1 && r.right <= w + 1;
    });
    expect(
      visibleBefore,
      "the filter box must be FULLY on screen, or declining to refocus is correct",
    ).toBe(true);

    expect(
      await waitForRefresh(page),
      "the refresh did not complete: \"no-refresh\" means <main> was never " +
        "swapped, \"no-restore\" means it was but restoreTableFilters did " +
        "not run, \"no-fixture\" means the route intercept stopped " +
        "matching. The assertions below would be measuring the wrong " +
        "page in every case",
    ).toBe("refreshed");

    /* Scoped to the fixture's own box, like the two tests above. The page can
       render several `.tf-input` elements once the peers and contracts cards
       have rows, so an unscoped class check answers a question about the page
       rather than about the element under test. The value and caret assertions
       below would have caught a wrong box, but only indirectly. */
    const state = await page.evaluate((id) => {
      const el = document.activeElement as HTMLInputElement | null;
      const wrap = el && el.closest ? el.closest(".table-filter") : null;
      return {
        focused:
          !!(el && el.classList && el.classList.contains("tf-input")) &&
          !!wrap &&
          wrap.getAttribute("data-filter-for") === id,
        value: el ? el.value : null,
        caret: el ? el.selectionStart : null,
      };
    }, FIXTURE_TABLE_ID);
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
