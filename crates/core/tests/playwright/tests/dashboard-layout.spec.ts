import { test, expect } from "@playwright/test";

// Regression tests for the local dashboard's responsive layout
// (`home_page/assets/style.css`).
//
// Why this lives in Playwright and not in Rust. These are assertions about
// COMPUTED layout — how many columns a grid resolves to, and whether the page
// scrolls sideways. Nothing in Rust can see either: the Rust tests assert the
// emitted markup and never run a layout engine, so a CSS rule that is present
// in the file but never APPLIES is indistinguishable from one that works.
//
// That is not hypothetical, it is the exact bug these guard. Two separate
// fixes in this area shipped DEAD:
//
//   1. `.g-norms` had responsive column counts at 768px and 400px which had
//      never once applied — the unconditional base rule sits later in the file
//      at equal specificity, so it won at every width. The page scrolled
//      sideways on every phone regardless, and the fix looked correct in
//      review because the rule was right there in the source.
//   2. the first attempt at fixing `.g-verdict-row` was placed in an earlier
//      media block and lost the cascade the same way, computing to
//      `280px 87.64px` at 390px and changing nothing.
//
// A reader cannot tell a live rule from a dead one by skimming; only the
// computed style can. Hence these tests assert computed results, never the
// presence of a declaration.

const shellUrl = process.env.FREENET_SHELL_URL;
const dashboardUrl = shellUrl ? new URL("/", shellUrl).toString() : undefined;

test.skip(
  !shellUrl,
  "FREENET_SHELL_URL is not set — run via `cargo test --test playwright_shell`",
);

test.describe("dashboard responsive layout", () => {
  test("the page does not scroll sideways on a narrow phone", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 320, height: 800 });
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const { scrollWidth, clientWidth } = await page.evaluate(() => ({
      scrollWidth: document.documentElement.scrollWidth,
      clientWidth: document.documentElement.clientWidth,
    }));
    // Tables are exempt by design: `.table-wrap` sets `overflow-x: auto`, so
    // they scroll INSIDE their own box. What must never happen is the whole
    // document scrolling.
    expect(
      scrollWidth,
      `document scrolls horizontally: scrollWidth ${scrollWidth} > clientWidth ${clientWidth}`,
    ).toBeLessThanOrEqual(clientWidth);
  });

  // 768px, not 320px, is where the responsive rules actually bite, and picking
  // the wrong width would have made this test useless. `auto-fit` with a 130px
  // minimum independently yields two columns on a 320px phone, so the
  // breakpoints could be deleted entirely and a 320px assertion would still
  // pass. Measured with the breakpoints removed: 320px still gives 2 columns,
  // while 768px jumps to 5. Assert at the width that discriminates.
  test("stat tiles do not pack five across at tablet width", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 768, height: 900 });
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const columnCounts = await page.evaluate(() =>
      [...document.querySelectorAll(".g-norms")].map(
        (n) => getComputedStyle(n).gridTemplateColumns.split(" ").length,
      ),
    );
    expect(
      columnCounts.length,
      "no .g-norms grids on the page — this test has stopped covering anything",
    ).toBeGreaterThan(0);
    // The 768px breakpoint asks for three. Five ~130px columns is too dense
    // for labels like "CONTRACT-CAP REJECTS", and five is exactly what the
    // dead-cascade bug produced here.
    for (const cols of columnCounts) {
      expect(
        cols,
        `a stat grid resolved to ${cols} columns at 768px wide`,
      ).toBeLessThanOrEqual(3);
    }
  });

  test("a stat grid that is its row's only child spans the full row", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const grids = await page.evaluate(() =>
      [...document.querySelectorAll(".g-verdict-row > .g-norms")].map((n) => {
        const parent = n.parentElement!;
        const tiles = [...n.querySelectorAll(".g-norm")];
        return {
          soleChild: parent.children.length === 1,
          parentWidth: Math.round(parent.getBoundingClientRect().width),
          width: Math.round(n.getBoundingClientRect().width),
          tiles: tiles.length,
          // Distinct top offsets = how many rows the tiles actually wrapped to.
          renderedRows: new Set(
            tiles.map((t) => Math.round(t.getBoundingClientRect().top)),
          ).size,
        };
      }),
    );
    expect(
      grids.length,
      "no .g-verdict-row > .g-norms grids — this test has stopped covering anything",
    ).toBeGreaterThan(0);

    const sole = grids.filter((g) => g.soleChild);
    expect(
      sole.length,
      "the hosting card renders stat grids as the only child of their row",
    ).toBeGreaterThan(0);

    for (const g of sole) {
      // `.g-verdict-row` reserves `minmax(280px, 0.85fr) 2fr` for a verdict box
      // beside a stat grid. Where the verdict box is absent, the grid was
      // stranded in track 1 at 280px of a 726px row, wrapping five tiles onto
      // three lines with 446px empty beside them.
      expect(
        g.parentWidth - g.width,
        `a sole stat grid is ${g.width}px inside a ${g.parentWidth}px row`,
      ).toBeLessThan(20);
      expect(
        g.renderedRows,
        `${g.tiles} tiles wrapped onto ${g.renderedRows} rows at desktop width`,
      ).toBe(1);
    }
  });
});
