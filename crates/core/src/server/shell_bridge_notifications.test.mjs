// Executable unit test for the `makeNotifyRateLimiter` factory in
// `path_handlers/assets/shell_bridge.js` — the notification proxy's rate limiter
// (freenet/river#408).
//
// Why this exists: the limiter is intricate stateful threshold logic (a per-tag
// throttle + a rolling global cap + bounded-map eviction) that runs in the
// browser shell, where the surrounding IIFE touches browser-only globals and so
// can't run under Node. Rather than pull in a browser runner, this EXTRACTS the
// self-contained factory verbatim from the asset (between the
// `notify-rate-limiter:BEGIN`/`:END` markers — the same technique
// base58_encode.test.mjs uses) and drives its `ok(tag, now)` with an INJECTED
// clock to verify the boundary behavior the source-pin tests can't.
//
// Run via `npm test` in crates/core/src/server (wired into the lint-assets CI
// job). No dependencies beyond Node's stdlib. Exits non-zero on any mismatch.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'path_handlers/assets/shell_bridge.js');
const src = readFileSync(assetPath, 'utf8');

// --- Extract the factory verbatim from the asset -------------------------
const BEGIN = 'notify-rate-limiter:BEGIN';
const END = 'notify-rate-limiter:END';
const b = src.indexOf(BEGIN);
const e = src.indexOf(END);
if (b < 0 || e < 0 || e < b) {
  console.error(
    `FAIL: could not find ${BEGIN}/${END} markers in ${assetPath}. The ` +
      'rate limiter must stay bracketed by those markers so this test can ' +
      'extract and verify it.',
  );
  process.exit(1);
}
const region = src.slice(b, e);
const fnStart = region.indexOf('function makeNotifyRateLimiter(');
if (fnStart < 0) {
  console.error('FAIL: no `function makeNotifyRateLimiter(` between the markers.');
  process.exit(1);
}
const fnSource = region.slice(fnStart);
const makeNotifyRateLimiter = new Function(
  `${fnSource}\nreturn makeNotifyRateLimiter;`,
)();

let failures = 0;
function check(name, cond) {
  if (cond) {
    console.log(`  ok   ${name}`);
  } else {
    console.error(`  FAIL ${name}`);
    failures++;
  }
}

// 1. Per-tag throttle: the same tag within 3s is blocked; at/after 3s allowed.
{
  const rl = makeNotifyRateLimiter();
  check('same tag t=0 allowed', rl.ok('a', 0) === true);
  check('same tag t=2999 blocked (throttle window)', rl.ok('a', 2999) === false);
  check('same tag t=3000 allowed (window elapsed)', rl.ok('a', 3000) === true);
}

// 2. Distinct tags (different rooms) within the throttle window are NOT dropped.
{
  const rl = makeNotifyRateLimiter();
  check('tag a t=0 allowed', rl.ok('a', 0) === true);
  check('tag b t=0 allowed (distinct room)', rl.ok('b', 0) === true);
  check('tag c t=100 allowed (distinct room)', rl.ok('c', 100) === true);
}

// 3. Global cap: 20 distinct tags in the 60s window pass, the 21st is blocked.
{
  const rl = makeNotifyRateLimiter();
  let passed = 0;
  for (let i = 0; i < 20; i++) if (rl.ok('tag' + i, 1000 + i)) passed++;
  check('first 20 distinct tags pass', passed === 20);
  check('21st distinct tag blocked by global cap', rl.ok('tag20', 1500) === false);
}

// 4. Rolling window: once the oldest entries age past 60s, capacity frees up.
{
  const rl = makeNotifyRateLimiter();
  for (let i = 0; i < 20; i++) rl.ok('t' + i, 0); // fill the window at t=0
  check('at capacity blocks a new tag at t=59999', rl.ok('new', 59999) === false);
  // At t=60000 the t=0 entries have aged out (now - t is no longer < 60000).
  check('window expiry frees capacity at t=60000', rl.ok('new', 60000) === true);
}

// 5. Bounded per-tag map: a flood of >128 distinct tags must not throw and the
//    limiter must keep working (eviction path). Space calls >60s apart so
//    neither the per-tag nor the global-window gate blocks them.
{
  const rl = makeNotifyRateLimiter();
  let allOk = true;
  for (let i = 0; i < 300; i++) {
    if (rl.ok('flood' + i, i * 61000) !== true) allOk = false;
  }
  check('300 spaced distinct tags all pass (eviction bounded, no throw)', allOk);
}

if (failures > 0) {
  console.error(`\nnotify-rate-limiter: ${failures} check(s) FAILED`);
  process.exit(1);
}
console.log('notify-rate-limiter: all checks passed');
