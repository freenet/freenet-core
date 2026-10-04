import { test, expect } from "@playwright/test";

// Things that are normal must not be PAINTED as alarms.
//
// The dashboard used to render several ordinary states in warning colours: an
// always-red operations failure count (including a red "0"), and an orange
// "next to evict" badge on a node far under budget. Both are now neutral, and
// the sentence that explains a full cache ("Full is normal here") has to be
// readable, because it carries the meaning a bar's colour used to.
//
// The Rust tests pin the stylesheet TEXT of these rules. They cannot tell
// whether the declaration wins the cascade or what it computes to under each
// theme — a `[data-theme='light']` override, or a later rule with the old
// colour, would pass them and still paint the element red. Only a browser
// resolving the real stylesheet can see that, so this asserts the COMPUTED
// colour, in both themes.

const shellUrl = process.env.FREENET_SHELL_URL;
const dashboardUrl = shellUrl ? new URL("/", shellUrl).toString() : undefined;

test.skip(
  !shellUrl,
  "FREENET_SHELL_URL is not set — run via `cargo test --test playwright_shell`",
);

type Rgb = { r: number; g: number; b: number };

function rgb(value: string): Rgb {
  const m = value.match(/rgba?\(([^)]+)\)/);
  if (!m) throw new Error(`not a colour: ${value}`);
  const [r, g, b] = m[1].split(",").map((p) => parseFloat(p.trim()));
  return { r, g, b };
}

/** Spread between the strongest and weakest channel. A grey is near 0; the
 *  alarm colours these rules used to carry are all above 130 (#f87171 is 135,
 *  #dc2626 is 182, #ff8a3d is 194). */
function chroma(c: Rgb): number {
  return Math.max(c.r, c.g, c.b) - Math.min(c.r, c.g, c.b);
}

function luminance(c: Rgb): number {
  const f = (v: number) => {
    const s = v / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  };
  return 0.2126 * f(c.r) + 0.7152 * f(c.g) + 0.0722 * f(c.b);
}

function contrast(a: Rgb, b: Rgb): number {
  const la = luminance(a);
  const lb = luminance(b);
  return (Math.max(la, lb) + 0.05) / (Math.min(la, lb) + 0.05);
}

/** Anything at or above this is a hue, not a grey. Far below the old alarm
 *  colours and far above the neutral text tokens (under 10 in both themes). */
const MAX_NEUTRAL_CHROMA = 40;

// Probes are injected rather than waiting for the node to reach each state.
// The rules under test are static stylesheet rules; what matters is what they
// compute to. Class names must match what `cards.rs` emits.
const PROBES: { name: string; className: string; mustBeReadable: boolean }[] = [
  { name: "operations failure count", className: "op-fail", mustBeReadable: true },
  { name: "next-to-evict badge", className: "hz-badge hz-next", mustBeReadable: false },
  { name: "closest-limit note", className: "hz-binding-note", mustBeReadable: true },
];

for (const scheme of ["dark", "light"] as const) {
  test.describe(`normal states are not painted as alarms (${scheme})`, () => {
    for (const probe of PROBES) {
      test(`${probe.name} is a neutral colour`, async ({ page }) => {
        await page.emulateMedia({ colorScheme: scheme });
        await page.goto(dashboardUrl!, { waitUntil: "domcontentloaded" });

        const result = await page.evaluate((className) => {
          const el = document.createElement("span");
          el.className = className;
          el.textContent = "probe";
          document.querySelector("main")!.appendChild(el);
          return {
            color: getComputedStyle(el).color,
            bodyBg: getComputedStyle(document.body).backgroundColor,
            stamped: document.documentElement.getAttribute("data-theme"),
          };
        }, probe.className);

        const colour = rgb(result.color);
        const bg = rgb(result.bodyBg);

        // Precondition: the page really is in the theme under test, or a
        // pass here says nothing about that theme's overrides.
        const bgLum = luminance(bg);
        if (scheme === "light") {
          expect(bgLum, "precondition: light page").toBeGreaterThan(0.5);
        } else {
          expect(bgLum, "precondition: dark page").toBeLessThan(0.2);
        }

        expect(
          chroma(colour),
          `.${probe.className} computes to ${result.color} under the ${scheme} theme ` +
            `(data-theme=${result.stamped}) — that is a hue, and this element ` +
            `describes a normal state, so it must be neutral`,
        ).toBeLessThan(MAX_NEUTRAL_CHROMA);

        if (probe.mustBeReadable) {
          const ratio = contrast(colour, bg);
          expect(
            ratio,
            `.${probe.className} has a contrast ratio of ${ratio.toFixed(2)} ` +
              `against the page background under the ${scheme} theme — neutral ` +
              `must not mean unreadable`,
          ).toBeGreaterThan(4.5);
        }
      });
    }
  });
}
