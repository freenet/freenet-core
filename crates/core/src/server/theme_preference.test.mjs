// Executable behavioural test for the dashboard's theme resolution in
// `home_page/assets/dashboard.js`.
//
// Why this exists: the stylesheet had no `prefers-color-scheme` rule at all,
// so the dashboard was dark for everyone until they found the sun icon, no
// matter what their operating system was set to.
//
// The reason that could not be fixed by adding a media query alone is the
// interesting part. The setting was effectively TWO-state: the toggle stored
// 'light' or REMOVED the key, so "I deliberately want dark" and "I have not
// chosen" were the same stored value. Adding an OS default would then have
// overridden someone who had explicitly picked dark on a light-mode desktop —
// silently reversing a choice they had made. Storing 'dark' explicitly is what
// makes the OS default safe.
//
// So the property under test is not "does light mode work" but: an explicit
// choice must beat the OS in BOTH directions, while its absence defers. A
// substring pin ("does the source mention prefers-color-scheme?") cannot
// distinguish those cases — the original bug would have passed such a pin the
// moment the media query was added, regardless of whether the guard was right.
//
// Following the pattern of dashboard_refresh.test.mjs and update_check.test.mjs,
// this EXTRACTS `resolveTheme` verbatim from the asset (between the
// `theme-resolve:BEGIN`/`:END` markers) and drives it under Node.
//
// Run via `npm test` in crates/core/src/server (wired into the lint-assets CI
// job). No dependencies beyond Node's stdlib. Exits non-zero on any mismatch.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'home_page/assets/dashboard.js');
const src = readFileSync(assetPath, 'utf8');

const BEGIN = 'theme-resolve:BEGIN';
const END = 'theme-resolve:END';
const b = src.indexOf(BEGIN);
const e = src.indexOf(END);
if (b < 0 || e < 0 || e < b) {
  console.error(
    `theme_preference.test.mjs: could not find the ${BEGIN}/${END} markers in ` +
      `dashboard.js. If the resolver was renamed or moved, update the markers ` +
      `rather than deleting this test — the extraction failing closed is the ` +
      `point, so a rename cannot silently leave the behaviour unguarded.`
  );
  process.exit(1);
}
// Take from the start of the line after the BEGIN comment block through END.
const region = src.slice(b, e);
const fnStart = region.indexOf('function resolveTheme');
if (fnStart < 0) {
  console.error(
    'theme_preference.test.mjs: `function resolveTheme` not found between the markers.'
  );
  process.exit(1);
}
// The region runs up to the END marker, which sits inside its own comment —
// so trim back to the function's own closing brace or `new Function` chokes on
// a dangling `/*`.
const raw = region.slice(fnStart);
const lastBrace = raw.lastIndexOf('}');
if (lastBrace < 0) {
  console.error('theme_preference.test.mjs: could not find the end of resolveTheme.');
  process.exit(1);
}
const body = raw.slice(0, lastBrace + 1);

// eslint-disable-next-line no-new-func
const resolveTheme = new Function(`${body}; return resolveTheme;`)();

let failures = 0;
function check(name, actual, expected) {
  const ok = actual === expected;
  if (!ok) failures++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}` + (ok ? '' : ` — got ${actual}, want ${expected}`));
}

// No explicit choice: defer to the OS. This is the case that did not exist
// before — the page was dark regardless.
check('no choice + OS light  → light', resolveTheme(null, true), 'light');
check('no choice + OS dark   → dark', resolveTheme(null, false), 'dark');

// An explicit choice must win in BOTH directions. The second of these is the
// one the old two-state model could not represent at all.
check('explicit light beats a dark OS', resolveTheme('light', false), 'light');
check('explicit dark beats a light OS', resolveTheme('dark', true), 'dark');

// Agreement between choice and OS is not special-cased.
check('explicit light + OS light', resolveTheme('light', true), 'light');
check('explicit dark + OS dark', resolveTheme('dark', false), 'dark');

// A junk or legacy attribute value must not be treated as a choice; it should
// fall through to the OS rather than pinning some arbitrary theme.
check('unknown attribute defers to OS light', resolveTheme('chartreuse', true), 'light');
check('unknown attribute defers to OS dark', resolveTheme('chartreuse', false), 'dark');
check('empty attribute defers to OS', resolveTheme('', true), 'light');
check('undefined attribute defers to OS', resolveTheme(undefined, true), 'light');

console.log(failures === 0 ? '\nAll theme-resolution cases passed.' : `\n${failures} failed.`);
process.exit(failures === 0 ? 0 : 1);
