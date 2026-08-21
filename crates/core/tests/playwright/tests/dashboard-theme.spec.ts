import { test, expect, type Page } from "@playwright/test";

// Regression tests for OS-theme following on the local dashboard.
//
// The bug these exist for is not "the theme is wrong" but "the theme is HALF
// applied". The first version of this change resolved the OS preference by
// redefining the `:root` colour variables inside a `prefers-color-scheme`
// block, and left `data-theme` unstamped when the operator had made no
// explicit choice. But 24 rules in `style.css` key their light styling off
// `[data-theme='light']` directly with hardcoded colours — warning banners,
// NAT diagnostics, health banners, verdict badges — so an OS-light operator
// got light page chrome with dark-locked components sitting on it:
// `.warning` rendering #fbbf24 amber on a #f7f5f2 background.
//
// The Node test (`theme_preference.test.mjs`) drives the resolution function
// and would happily have passed throughout: it knows nothing about which CSS
// rules the resolved value actually reaches. Only a browser can see that, and
// only if it emulates the OS preference AND renders a component that has its
// own light override. The original validation did neither, which is why it
// reported 10/10 on a page that was half dark.

const shellUrl = process.env.FREENET_SHELL_URL;
const dashboardUrl = shellUrl ? new URL("/", shellUrl).toString() : undefined;

test.skip(
  !shellUrl,
  "FREENET_SHELL_URL is not set — run via `cargo test --test playwright_shell`",
);

/** Parse `rgb(r, g, b)` / `rgba(...)` into components. */
function rgb(value: string): { r: number; g: number; b: number } {
  const m = value.match(/rgba?\(([^)]+)\)/);
  if (!m) throw new Error(`not a colour: ${value}`);
  const [r, g, b] = m[1].split(",").map((p) => parseFloat(p.trim()));
  return { r, g, b };
}

/** WCAG relative luminance, used only to tell "dark" from "light". */
function luminance(c: { r: number; g: number; b: number }): number {
  const f = (v: number) => {
    const s = v / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  };
  return 0.2126 * f(c.r) + 0.7152 * f(c.g) + 0.0722 * f(c.b);
}

async function bodyBackgroundLuminance(page: Page): Promise<number> {
  const bg = await page.evaluate(
    () => getComputedStyle(document.body).backgroundColor,
  );
  return luminance(rgb(bg));
}

test.describe("dashboard OS theme following", () => {
  test("an OS light preference with no explicit choice yields a light page", async ({
    page,
  }) => {
    await page.emulateMedia({ colorScheme: "light" });
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const lum = await bodyBackgroundLuminance(page);
    expect(
      lum,
      `body background is not light under an OS light preference (luminance ${lum.toFixed(3)})`,
    ).toBeGreaterThan(0.5);
  });

  test("an OS dark preference with no explicit choice yields a dark page", async ({
    page,
  }) => {
    await page.emulateMedia({ colorScheme: "dark" });
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const lum = await bodyBackgroundLuminance(page);
    expect(
      lum,
      `body background is not dark under an OS dark preference (luminance ${lum.toFixed(3)})`,
    ).toBeLessThan(0.2);
  });

  /* The finding this file exists for. A component with its own
   * `[data-theme='light']` override must actually RECEIVE it when the light
   * theme was reached via the OS rather than via the toggle. */
  test("components with light overrides are themed under an OS light preference", async ({
    page,
  }) => {
    await page.emulateMedia({ colorScheme: "light" });
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

    const result = await page.evaluate(() => {
      // Inject one of the affected components rather than waiting for the node
      // to enter a NAT-trouble or version-mismatch state. The rule under test
      // is a static stylesheet rule; what matters is that the selector matches.
      const el = document.createElement("div");
      el.className = "warning";
      el.textContent = "probe";
      document.querySelector("main")!.appendChild(el);
      const cs = getComputedStyle(el);
      return {
        color: cs.color,
        bodyBg: getComputedStyle(document.body).backgroundColor,
        stamped: document.documentElement.getAttribute("data-theme"),
      };
    });

    /* CONTRAST, not a luminance comparison — and the difference is not
       academic. The first version of this test asserted "text darker than
       background", which the broken page PASSES: the dark-theme amber
       (#fbbf24, luminance 0.60) is genuinely darker than the light background
       (#f7f5f2, 0.92). It just has a contrast ratio of about 1.5 against it,
       where the light override (#92400e) gives about 8.8. Mutation-testing the
       assertion is what exposed that; the reverted shape sailed through it. */
    const textLum = luminance(rgb(result.color));
    const bgLum = luminance(rgb(result.bodyBg));
    const ratio =
      (Math.max(textLum, bgLum) + 0.05) / (Math.min(textLum, bgLum) + 0.05);

    expect(
      bgLum,
      "precondition: the page must be light for this test to mean anything",
    ).toBeGreaterThan(0.5);
    // WCAG AA for normal text is 4.5; 3.0 leaves room for palette tuning while
    // still being far above the ~1.5 the unthemed component scores.
    expect(
      ratio,
      `.warning has a contrast ratio of ${ratio.toFixed(2)} against the page ` +
        `background — its [data-theme='light'] override did not apply, so the ` +
        `theme is only half-resolved. data-theme=${result.stamped}, ` +
        `color=${result.color}, bg=${result.bodyBg}`,
    ).toBeGreaterThan(3.0);
  });

  /* The live listener, which nothing else here reaches.
   *
   * Every other test in this file calls `emulateMedia` BEFORE `goto`/`reload`,
   * so they all re-test the load-time resolver. Review pointed out that
   * deleting `watchOsTheme()` outright would not have failed a single one of
   * them — the function the second commit is named after was completely
   * unguarded.
   *
   * The listener is not optional polish, either: stamping the resolved theme
   * onto `data-theme` is what makes the 24 attribute-keyed component rules
   * work, and it also stops the `prefers-color-scheme` block from matching. So
   * without this listener a stamped page ignores the OS until reload —
   * stamping traded a live-follow behaviour away, and this earns it back. */
  test("a live OS theme change is followed without a reload", async ({
    page,
  }) => {
    await page.emulateMedia({ colorScheme: "dark" });
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });
    expect(
      await bodyBackgroundLuminance(page),
      "precondition: starts dark",
    ).toBeLessThan(0.2);

    const iconBefore = await page.textContent("#theme-icon");

    // Flip the OS preference with the page already open. No reload.
    await page.emulateMedia({ colorScheme: "light" });
    await page.waitForFunction(
      () => {
        const bg = getComputedStyle(document.body).backgroundColor;
        const m = bg.match(/rgba?\(([^)]+)\)/);
        if (!m) return false;
        const [r, g, b] = m[1].split(",").map((x) => parseFloat(x.trim()));
        // Crude but sufficient: a light background is bright in all channels.
        return r > 200 && g > 200 && b > 200;
      },
      undefined,
      { timeout: 5000 },
    );

    expect(
      await bodyBackgroundLuminance(page),
      "the page must follow a live OS flip without a reload",
    ).toBeGreaterThan(0.5);

    // The icon advertises what the NEXT click does, so it has to move too —
    // otherwise it promises the opposite of what it delivers.
    const iconAfter = await page.textContent("#theme-icon");
    expect(
      iconAfter,
      `the toggle icon still reads ${iconAfter} after the OS flipped to light`,
    ).not.toBe(iconBefore);
  });

  /* The other half: an explicit choice must NOT be overridden by the OS
   * changing underneath it. Without this, a listener that re-stamps
   * unconditionally would pass the test above while silently discarding the
   * operator's decision. */
  test("a live OS change does not override an explicit choice", async ({
    page,
  }) => {
    await page.emulateMedia({ colorScheme: "dark" });
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });
    await page.evaluate(() => localStorage.setItem("theme", "dark"));
    await page.reload({ waitUntil: "domcontentloaded" });
    expect(
      await bodyBackgroundLuminance(page),
      "precondition: explicit dark",
    ).toBeLessThan(0.2);

    await page.emulateMedia({ colorScheme: "light" });
    await page.waitForTimeout(500);

    expect(
      await bodyBackgroundLuminance(page),
      "an explicit dark choice must survive the OS flipping to light while " +
        "the page is open",
    ).toBeLessThan(0.2);
  });

  test("an explicit choice beats the OS in both directions", async ({
    page,
  }) => {
    // Explicit dark on a light-preferring OS.
    await page.emulateMedia({ colorScheme: "light" });
    await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });
    await page.evaluate(() => localStorage.setItem("theme", "dark"));
    await page.reload({ waitUntil: "domcontentloaded" });
    expect(
      await bodyBackgroundLuminance(page),
      "an explicit dark choice must survive a light-preferring OS",
    ).toBeLessThan(0.2);

    // Explicit light on a dark-preferring OS.
    await page.emulateMedia({ colorScheme: "dark" });
    await page.evaluate(() => localStorage.setItem("theme", "light"));
    await page.reload({ waitUntil: "domcontentloaded" });
    expect(
      await bodyBackgroundLuminance(page),
      "an explicit light choice must survive a dark-preferring OS",
    ).toBeGreaterThan(0.5);

    // Clearing the choice returns control to the OS.
    await page.evaluate(() => localStorage.removeItem("theme"));
    await page.reload({ waitUntil: "domcontentloaded" });
    expect(
      await bodyBackgroundLuminance(page),
      "with the choice cleared the OS preference must take over again",
    ).toBeLessThan(0.2);
  });
});
