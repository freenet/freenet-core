// Executable behavioral test for the dashboard update-check cache in
// `home_page/assets/dashboard.js`.
//
// Why this exists: #5102. The dashboard's "is there a newer release" check
// runs in the browser and — unlike the node's own poll, which moved to the
// quota-free github.com redirect — must stay on api.github.com, because that
// redirect sends no CORS headers. So it spends from the same 60 requests/hour
// per-IP REST budget, a budget shared with every other machine behind the same
// NAT/CGNAT or VPN exit.
//
// The original bug: only SUCCESS was cached. Once GitHub started refusing, the
// cache was never written, so every single page reload issued another doomed
// request — the one client that should have gone quiet instead knocked hardest
// precisely while the IP was already being rate-limited.
//
// A substring pin ("does the source mention failedAt?") cannot catch a wiring
// error in a small state machine like this, so — following the pattern of
// dashboard_refresh.test.mjs — this EXTRACTS the `createUpdateChecker` factory
// verbatim from the asset (between the `update-check:BEGIN`/`:END` markers; it
// takes all browser deps as injected params) and drives it under Node with a
// fake clock, fake storage and a controllable fake fetch.
//
// Run via `npm test` in crates/core/src/server (wired into the lint-assets CI
// job). No dependencies beyond Node's stdlib. Exits non-zero on any mismatch.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'home_page/assets/dashboard.js');
const src = readFileSync(assetPath, 'utf8');

// --- Extract the factory verbatim from the asset --------------------------
const BEGIN = 'update-check:BEGIN';
const END = 'update-check:END';
const b = src.indexOf(BEGIN);
const e = src.indexOf(END);
if (b < 0 || e < 0 || e < b) {
  console.error(
    `FAIL: could not find ${BEGIN}/${END} markers in ${assetPath}. ` +
      'The update-check core must stay bracketed by those markers so this ' +
      'test can extract and verify it.',
  );
  process.exit(1);
}
const region = src.slice(b, e);
const fnStart = region.indexOf('function createUpdateChecker(');
if (fnStart < 0) {
  console.error(
    'FAIL: no `function createUpdateChecker(` between the markers.',
  );
  process.exit(1);
}
const fnSource = region.slice(fnStart, region.lastIndexOf('}') + 1);
const createUpdateChecker = new Function(
  `${fnSource}\nreturn createUpdateChecker;`,
)();

// --- Harness ---------------------------------------------------------------
const HOUR = 60 * 60 * 1000;
let failures = 0;
function ok(cond, msg) {
  if (cond) {
    console.log('  ok  ', msg);
  } else {
    failures++;
    console.error('  FAIL', msg);
  }
}

function harness(initialNow) {
  const state = {
    now: initialNow,
    store: new Map(),
    fetches: 0,
    badges: [],
    // default: succeed with 9.9.9
    respond: () => Promise.resolve({ tag_name: 'v9.9.9' }),
  };
  const checker = createUpdateChecker({
    now: () => state.now,
    getItem: (k) => (state.store.has(k) ? state.store.get(k) : null),
    setItem: (k, v) => state.store.set(k, v),
    // Only the ordering matters here, not real semver parsing.
    compareSemver: (a, bb) => (String(a) === String(bb) ? 0 : 1),
    showBadge: (t) => state.badges.push(t),
    fetchLatest: () => {
      state.fetches++;
      return state.respond();
    },
  });
  return { state, checker };
}

// --- 1. A failure is remembered and suppresses further requests ------------
{
  const { state, checker } = harness(1_000_000);
  state.respond = () => Promise.reject(new Error('HTTP 429'));

  await checker.check('0.2.118');
  ok(state.fetches === 1, 'first check issues one request');
  ok(
    state.store.has('freenet-update-check'),
    'a FAILED check is written to storage (the #5102 fix — previously only success was)',
  );

  // Simulate page reloads: each constructs a fresh checker over the same store.
  for (let i = 0; i < 5; i++) {
    const again = createUpdateChecker({
      now: () => state.now + 1000 * i,
      getItem: (k) => (state.store.has(k) ? state.store.get(k) : null),
      setItem: (k, v) => state.store.set(k, v),
      compareSemver: () => 1,
      showBadge: (t) => state.badges.push(t),
      fetchLatest: () => {
        state.fetches++;
        return Promise.reject(new Error('HTTP 429'));
      },
    });
    const outcome = await again.check('0.2.118');
    ok(
      outcome === 'backoff',
      `reload ${i + 1} backs off instead of requesting`,
    );
  }
  ok(
    state.fetches === 1,
    'five reloads while rate-limited issue ZERO extra requests (was: one per reload)',
  );
}

// --- 2. The failure window expires, so a client recovers unaided -----------
{
  const { state, checker } = harness(1_000_000);
  state.respond = () => Promise.reject(new Error('HTTP 429'));
  await checker.check('0.2.118');
  ok(state.fetches === 1, 'initial failing check requested once');

  const mk = (nowAt) =>
    createUpdateChecker({
      now: () => nowAt,
      getItem: (k) => (state.store.has(k) ? state.store.get(k) : null),
      setItem: (k, v) => state.store.set(k, v),
      compareSemver: () => 1,
      showBadge: (t) => state.badges.push(t),
      fetchLatest: () => {
        state.fetches++;
        return Promise.resolve({ tag_name: 'v9.9.9' });
      },
    });

  ok(
    (await mk(1_000_000 + HOUR - 1).check('0.2.118')) === 'backoff',
    'one millisecond before the window closes, still quiet',
  );
  ok(state.fetches === 1, 'no request issued inside the failure window');

  ok(
    (await mk(1_000_000 + HOUR).check('0.2.118')) === 'fetched',
    'at the window edge the client retries on its own',
  );
  ok(state.fetches === 2, 'exactly one retry after the window elapsed');
}

// --- 3. A fresh success is served from cache, without a request ------------
{
  const { state, checker } = harness(5_000_000);
  ok((await checker.check('0.2.118')) === 'fetched', 'first check fetches');
  ok(state.fetches === 1, 'one request on a cold cache');
  ok(state.badges.length === 1, 'a newer tag shows the badge');

  const cached = createUpdateChecker({
    now: () => 5_000_000 + 60_000,
    getItem: (k) => (state.store.has(k) ? state.store.get(k) : null),
    setItem: (k, v) => state.store.set(k, v),
    compareSemver: () => 1,
    showBadge: (t) => state.badges.push(t),
    fetchLatest: () => {
      state.fetches++;
      return Promise.resolve({ tag_name: 'v9.9.9' });
    },
  });
  ok(
    (await cached.check('0.2.118')) === 'cached',
    'a reload inside the success TTL serves from cache',
  );
  ok(state.fetches === 1, 'no extra request while the success cache is fresh');
}

// --- 4. A 200 with no usable tag counts as a failure -----------------------
{
  const { state, checker } = harness(7_000_000);
  state.respond = () => Promise.resolve({ message: 'API rate limit exceeded' });

  ok(
    (await checker.check('0.2.118')) === 'failed',
    'a 200 carrying no tag_name is treated as a failure',
  );
  const entry = JSON.parse(state.store.get('freenet-update-check'));
  ok(
    entry.failedAt === 7_000_000 && !entry.tag,
    'a tagless response records failedAt, so it cannot drive a per-reload retry loop',
  );
  ok(state.badges.length === 0, 'no badge is shown for a tagless response');
}

// --- 5. An unknown current version never requests --------------------------
{
  const { state, checker } = harness(9_000_000);
  ok(
    (await checker.check('?')) === 'skipped',
    "the '?' placeholder version skips the check entirely",
  );
  ok((await checker.check('')) === 'skipped', 'an empty version skips too');
  ok(state.fetches === 0, 'an unknown current version issues no request');
}

// --- 6. Storage being unavailable must not throw ---------------------------
{
  let fetches = 0;
  const checker = createUpdateChecker({
    now: () => 1,
    getItem: () => {
      throw new Error('SecurityError: localStorage disabled');
    },
    setItem: () => {
      throw new Error('QuotaExceededError');
    },
    compareSemver: () => 1,
    showBadge: () => {},
    fetchLatest: () => {
      fetches++;
      return Promise.resolve({ tag_name: 'v9.9.9' });
    },
  });
  const outcome = await checker.check('0.2.118');
  ok(
    outcome === 'fetched' && fetches === 1,
    'private-mode storage failures degrade to no caching rather than throwing',
  );
}

if (failures > 0) {
  console.error(`update-check: ${failures} check(s) failed`);
  process.exit(1);
}
console.log('update-check: all checks passed');
