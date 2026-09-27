// Executable unit test for the connecting page's bounded recovery logic in
// `errors/assets/connecting.html` (PR #4781, MAJOR #3): the retry-in-place must
// be TIME-BOUNDED (no 3s-forever loop). `connectingRecoveryDecision` returns the
// action for a given (`location.search`, `atTop`, `now`, `location.pathname`):
//
//   - no `_freload`           -> top-level non-app: 'dashboard'; app or framed:
//                                'stamp' (start a bounded window, then retry)
//   - `_freload` within 2 min -> 'retry' (reload the contract URL in place)
//   - `_freload` past 2 min   -> top-level: 'dashboard'; framed: 'giveup' (stop
//                                the loop, show a "reload the tab" message)
//
// Extract-verbatim-between-markers technique, same as the shell_bridge tests.
// Run via `npm test` in crates/core/src/server (wired into the lint-assets CI
// job). Exits non-zero on any mismatch.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { runInNewContext } from 'node:vm';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'errors/assets/connecting.html');
const src = readFileSync(assetPath, 'utf8');

const BEGIN = 'connecting-recovery-decision:BEGIN';
const END = 'connecting-recovery-decision:END';
const b = src.indexOf(BEGIN);
const e = src.indexOf(END);
if (b < 0 || e < 0 || e < b) {
  console.error(
    `FAIL: could not find ${BEGIN}/${END} markers in ${assetPath}. The ` +
      'decision function must stay bracketed so this test can extract it.',
  );
  process.exit(1);
}
const region = src.slice(b, e);
const fnStart = region.indexOf('function connectingRecoveryDecision(');
if (fnStart < 0) {
  console.error(
    'FAIL: no `function connectingRecoveryDecision(` between the markers.',
  );
  process.exit(1);
}
const connectingRecoveryDecision = new Function(
  `${region.slice(fnStart)}\nreturn connectingRecoveryDecision;`,
)();

let failures = 0;
let checks = 0;
function check(name, cond) {
  checks++;
  if (cond) {
    console.log(`  ok   ${name}`);
  } else {
    console.error(`  FAIL ${name}`);
    failures++;
  }
}

const NOW = 5_000_000;
const WINDOW_MS = 120000; // must match RECOVERY_MAX_MS in connecting.html

// No recovery marker: ordinary connecting page.
check(
  'top-level root, no _freload -> dashboard',
  connectingRecoveryDecision('', true, NOW, '/').action === 'dashboard',
);

const APP_PATH = '/v1/contract/web/shared-key/';
check(
  'top-level app, no _freload -> stamp (preserve shared app link)',
  connectingRecoveryDecision('', true, NOW, APP_PATH).action === 'stamp',
);
for (const path of [
  '/status',
  '/v1/contract/web',
  '/v1/contract/website/key/',
]) {
  check(
    `top-level non-app ${path}, no _freload -> dashboard`,
    connectingRecoveryDecision('', true, NOW, path).action === 'dashboard',
  );
}
for (const elapsed of [0, WINDOW_MS - 1, WINDOW_MS, WINDOW_MS + 1]) {
  check(
    `top-level app recovery at ${elapsed}ms -> bounded retry`,
    connectingRecoveryDecision(
      `?_freload=${NOW - elapsed}-0`,
      true,
      NOW,
      APP_PATH,
    ).action === (elapsed < WINDOW_MS ? 'retry' : 'dashboard'),
  );
}

// Execute the whole script too: the URL must reach the decision function and
// the stamp action must preserve the app path, other parameters and fragment.
{
  const location = new URL(
    `http://localhost:7509${APP_PATH}?view=chat#message`,
  );
  let replaced;
  location.replace = (url) => {
    replaced = url;
  };
  location.reload = () => {
    throw new Error('first recovery must stamp its start');
  };
  const window = { location };
  window.top = window.self = window;
  const script = src.match(/<script>([\s\S]*?)<\/script>/)[1];
  runInNewContext(script, {
    window,
    location,
    URL,
    Date: { now: () => NOW },
    setTimeout: (fn) => fn(),
  });
  check(
    'top-level app reload stamps _freload and preserves the full shared URL',
    replaced ===
      `${location.origin}${APP_PATH}?view=chat&_freload=${NOW}-0#message`,
  );
}
check(
  'no-JS fallback unchanged: the only meta refresh is the <noscript> dashboard one',
  /<noscript><meta http-equiv="refresh" content="3;url=\/" \/><\/noscript>/.test(
    src,
  ) &&
    (src.match(/<meta\s[^>]*http-equiv=["']refresh["']/gi) || []).length === 1,
);
check(
  'framed, no _freload -> stamp (start a bounded window)',
  connectingRecoveryDecision('', false, NOW).action === 'stamp',
);

// Recovery marker within the window -> retry in place (top OR framed).
{
  const search = '?_freload=' + (NOW - 1000) + '-1';
  check(
    'top-level recovery within window -> retry (not dashboard: no app-loss)',
    connectingRecoveryDecision(search, true, NOW).action === 'retry',
  );
  check(
    'framed recovery within window -> retry',
    connectingRecoveryDecision(search, false, NOW).action === 'retry',
  );
}

// Recovery marker PAST the window -> stop looping (bounded).
{
  const search = '?_freload=' + (NOW - WINDOW_MS - 1) + '-1';
  check(
    'top-level recovery past window -> dashboard (bounded, no forever loop)',
    connectingRecoveryDecision(search, true, NOW).action === 'dashboard',
  );
  check(
    'framed recovery past window -> giveup (stop, show reload-the-tab message)',
    connectingRecoveryDecision(search, false, NOW).action === 'giveup',
  );
}

// Boundary: exactly at the window edge counts as elapsed (>= RECOVERY_MAX_MS).
{
  const search = '?_freload=' + (NOW - WINDOW_MS) + '-1';
  check(
    'exactly at the window edge is treated as elapsed',
    connectingRecoveryDecision(search, false, NOW).action === 'giveup',
  );
}

// A future-dated start (backward clock correction) is not treated as a live
// window: it stops rather than looping forever.
{
  const search = '?_freload=' + (NOW + 60000) + '-0';
  check(
    'future-dated recovery start does not loop (framed -> giveup)',
    connectingRecoveryDecision(search, false, NOW).action === 'giveup',
  );
}

// The bare-timestamp form (no "-count", as a framed stamp writes) also parses.
{
  const search = '?_freload=' + (NOW - 1000);
  check(
    'bare-timestamp _freload within window -> retry',
    connectingRecoveryDecision(search, true, NOW).action === 'retry',
  );
}

// `_freload` mixed with other query params is still recognized.
{
  const search = '?foo=bar&_freload=' + (NOW - 1000) + '-2&baz=1';
  check(
    '_freload among other params is recognized (retry within window)',
    connectingRecoveryDecision(search, true, NOW).action === 'retry',
  );
}

if (failures > 0) {
  console.error(
    `\nconnecting-recovery: ${checks - failures} passed, ${failures} failed`,
  );
  process.exit(1);
}
console.log(`connecting-recovery: ${checks} passed, 0 failed`);
