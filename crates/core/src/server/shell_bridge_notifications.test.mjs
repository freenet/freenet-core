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

// --- Extract the sessionStorage adapter verbatim too --------------------
// `makeNotifyRateStore` is real production code (the Array.isArray guard, JSON
// round-trip, non-finite + future-timestamp filtering, fail-safe try/catch)
// that the injected in-memory store in case 6 never exercises. Extract it
// between its markers and drive it with stubbed `sessionStorage` /
// `contractConsentKey` so that behavior is actually tested.
const SBEGIN = 'notify-rate-store:BEGIN';
const SEND = 'notify-rate-store:END';
const sb = src.indexOf(SBEGIN);
const se = src.indexOf(SEND);
if (sb < 0 || se < 0 || se < sb) {
  console.error(
    `FAIL: could not find ${SBEGIN}/${SEND} markers in ${assetPath}. The ` +
      'sessionStorage adapter must stay bracketed so this test can drive it.',
  );
  process.exit(1);
}
const storeRegion = src.slice(sb, se);
const storeFnStart = storeRegion.indexOf('function makeNotifyRateStore(');
if (storeFnStart < 0) {
  console.error('FAIL: no `function makeNotifyRateStore(` between the store markers.');
  process.exit(1);
}
const storeFnSource = storeRegion.slice(storeFnStart);
// Build the adapter with controllable stubs. `back` is the raw sessionStorage
// backing map; `ctrl.throwing` simulates private-mode / quota throws; `path`
// drives contractConsentKey's key derivation (a non-contract path => null key).
function makeStoreHarness(path) {
  const back = {};
  const ctrl = { throwing: false };
  const sessionStorage = {
    getItem(k) {
      if (ctrl.throwing) throw new Error('boom');
      return k in back ? back[k] : null;
    },
    setItem(k, v) {
      if (ctrl.throwing) throw new Error('quota');
      back[k] = String(v);
    },
  };
  const contractConsentKey = () => {
    const m = String(path).match(/\/v[12]\/contract\/web\/([^/?#]+)/);
    return m ? 'freenet_notify:' + m[1] : null;
  };
  const makeNotifyRateStore = new Function(
    'sessionStorage',
    'contractConsentKey',
    `${storeFnSource}\nreturn makeNotifyRateStore;`,
  )(sessionStorage, contractConsentKey);
  const store = makeNotifyRateStore();
  const ckey = contractConsentKey();
  return { store, back, ctrl, key: ckey ? ckey + ':rate' : null };
}

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

// 5. Bounded per-tag map: a flood of >128 distinct tags must not throw, the
//    limiter must keep working, AND the per-tag map must stay bounded by the
//    MAP_CAP eviction. Space calls >60s apart so neither the per-tag throttle
//    nor the global-window gate blocks them. The tagCount assertion is what
//    actually exercises eviction: with only the "no throw" check, deleting the
//    MAP_CAP eviction block entirely would keep this test green (#4849 F3) —
//    an unbounded map is a memory leak the old test couldn't see.
{
  const rl = makeNotifyRateLimiter();
  let allOk = true;
  for (let i = 0; i < 300; i++) {
    if (rl.ok('flood' + i, i * 61000) !== true) allOk = false;
  }
  check('300 spaced distinct tags all pass (eviction bounded, no throw)', allOk);
  check('per-tag map stays bounded by MAP_CAP after the flood', rl.tagCount() <= 128);
}

// 6. Reload persistence (#4849): the rolling global window is rehydrated from
//    an injected store, so a full page RELOAD can't reset the flood cap. A
//    consented contract that fires the cap, then forces a reload, must NOT get
//    a fresh budget. The storeless control proves the persistence is what
//    closes the hole.
{
  let saved = null;
  const store = {
    load: () => (saved === null ? null : saved.slice()),
    save: (recent) => {
      saved = recent.slice();
    },
  };
  // First "page load": fill the rolling window to the global cap.
  const before = makeNotifyRateLimiter(store);
  let filled = 0;
  for (let i = 0; i < 20; i++) if (before.ok('t' + i, 1000 + i)) filled++;
  check('cap fills on first load', filled === 20);
  check('window persisted to store', Array.isArray(saved) && saved.length === 20);
  // Simulate the reload: a NEW limiter rehydrates from the same store. The next
  // notification (still inside the 60s window) must be blocked — no reset.
  const afterReload = makeNotifyRateLimiter(store);
  check(
    'rehydrated limiter still at cap after reload (flood-cap not reset)',
    afterReload.ok('post-reload', 1500) === false,
  );
  // Control: a storeless limiter DOES reset on reload — exactly the #4849 hole
  // the persisted store closes.
  const noStore = makeNotifyRateLimiter();
  check(
    'storeless limiter resets on reload (control — the bug the store fixes)',
    noStore.ok('post-reload', 1500) === true,
  );
  // A FULL-cap window rehydrated from BEFORE the 60s window must FREE capacity,
  // not stay stuck at cap: the stale entries age out on the first `ok` past the
  // window. Uses a full GLOBAL_MAX (20) window so the check is NON-VACUOUS —
  // with fewer than the cap, `ok` would return true even if the window filter
  // were broken. (A "trust the stored count" shortcut would wrongly suppress
  // all notifications across a >60s reload until tab close.)
  const staleStore = {
    load: () => Array.from({ length: 20 }, (_, i) => i),
    save: () => {},
  };
  const afterStale = makeNotifyRateLimiter(staleStore);
  check(
    'rehydrated full stale (>60s old) window frees capacity',
    afterStale.ok('fresh', 70000) === true,
  );
}

// 8. bfcache resync (#4849): resync() re-reads the persisted window so a
//    back-forward-cache-restored page — whose in-memory window is frozen and
//    whose IIFE never re-ran — adopts the authoritative store window instead of
//    firing on its stale in-memory one. Conservative: it must NEVER wipe the
//    in-memory window on an empty/missing store load.
{
  let stored = [];
  // save is a no-op here; the test drives `stored` directly to simulate another
  // same-contract page updating the shared store while this page is frozen.
  const store = { load: () => stored.slice(), save: () => {} };
  const restored = makeNotifyRateLimiter(store); // store empty at construct -> recent = []
  // Another page fills the store to cap while this one is frozen in bfcache.
  stored = Array.from({ length: 20 }, (_, i) => 1000 + i);
  restored.resync();
  check(
    'resync adopts the persisted window on bfcache restore',
    restored.ok('a', 1500) === false,
  );
  // Conservative guard: an empty store load must NOT wipe the adopted window
  // (else a bfcache restore against a cleared store would reset the cap — the
  // exact bug). This is the load-bearing `if (loaded && loaded.length)` branch.
  stored = [];
  restored.resync();
  check(
    'resync does not wipe the window on an empty store load',
    restored.ok('b', 1600) === false,
  );
}

// 7. The real sessionStorage adapter (makeNotifyRateStore) — behavioral. The
//    fail-safe guards here are production code the injected in-memory store in
//    case 6 never touches; the Array.isArray guard is the sharpest (without it
//    a non-array stored value flows into `recent` and `recent.filter(...)`
//    throws in `ok()`, breaking ALL notifications for the contract).
{
  const h = makeStoreHarness('/v2/contract/web/KEY/');
  // Round-trip: save then load returns the finite numbers.
  h.store.save([100, 200, 300]);
  const rt = h.store.load();
  check(
    'adapter round-trips a valid window',
    Array.isArray(rt) && rt.length === 3 && rt[0] === 100 && rt[2] === 300,
  );
  // Non-array stored value -> null (the sharp Array.isArray guard).
  h.back[h.key] = JSON.stringify('not-an-array');
  check('adapter returns null for a non-array stored value', h.store.load() === null);
  // Corrupt JSON -> null (fail safe).
  h.back[h.key] = '{not valid json';
  check('adapter returns null for corrupt JSON', h.store.load() === null);
  // Non-number / non-finite entries filtered out.
  h.back[h.key] = JSON.stringify([1, 'x', null, Infinity, 2]);
  const filtered = h.store.load();
  check(
    'adapter filters non-finite / non-number entries',
    Array.isArray(filtered) && filtered.length === 2 && filtered[0] === 1 && filtered[1] === 2,
  );
  // Future-dated timestamps (backward clock correction) clamped out.
  h.back[h.key] = JSON.stringify([1000, Date.now() + 10 * 60 * 1000]);
  const clamped = h.store.load();
  check(
    'adapter drops future-dated timestamps (clock rollback)',
    Array.isArray(clamped) && clamped.length === 1 && clamped[0] === 1000,
  );
  // Storage throwing (private mode / quota): load -> null, save -> no throw.
  h.ctrl.throwing = true;
  let saveThrew = false;
  try {
    h.store.save([1]);
  } catch (e) {
    saveThrew = true;
  }
  check('adapter save swallows storage errors', saveThrew === false);
  check('adapter load returns null when storage throws', h.store.load() === null);
  // No contract key -> null store (limiter runs in-memory, no persistence).
  const none = makeStoreHarness('/some/other/path');
  check('makeNotifyRateStore returns null when there is no contract key', none.store === null);
}

if (failures > 0) {
  console.error(`\nnotify-rate-limiter: ${failures} check(s) FAILED`);
  process.exit(1);
}
console.log('notify-rate-limiter: all checks passed');
