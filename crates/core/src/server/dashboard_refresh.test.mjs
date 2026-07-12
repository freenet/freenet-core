// Executable behavioral test for the dashboard auto-refresh scheduler in
// `home_page/assets/dashboard.js`.
//
// Why this exists: the scheduler is a small state machine (refreshTimer /
// refreshInFlight / visibility-dependent cadence) whose original bug — a
// visibilitychange racing an in-flight timer-triggered fetch started a second
// concurrent refresh chain, because refreshTimer was never reset once its
// setTimeout callback fired — is exactly the kind of wiring error a substring
// pin test cannot catch. Rather than pull in a browser test runner, this test
// EXTRACTS the self-contained `createRefreshScheduler` factory verbatim from
// the asset (between the `refresh-scheduler:BEGIN`/`:END` markers; the factory
// takes all browser deps — timers, refresh fn, visibility — as injected
// params) and drives it under Node with fake timers and a controllable fake
// refresh, asserting the behavioral invariants:
//
//   1. one fetch at a time: a visibility flip during an in-flight refresh
//      never starts a second concurrent refresh (the pre-fix bug);
//   2. cadence: hidden tabs are polled every 60s, visible tabs every 5s;
//   3. becoming visible triggers an immediate refresh only when idle,
//      otherwise it just reschedules;
//   4. the poll chain never forks: after arbitrary interleavings of timer
//      fires and visibility toggles, exactly one timer is pending at every
//      quiescent point.
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
const BEGIN = 'refresh-scheduler:BEGIN';
const END = 'refresh-scheduler:END';
const b = src.indexOf(BEGIN);
const e = src.indexOf(END);
if (b < 0 || e < 0 || e < b) {
  console.error(
    `FAIL: could not find ${BEGIN}/${END} markers in ${assetPath}. ` +
      'The refresh scheduler must stay bracketed by those markers so this ' +
      'test can extract and verify it.',
  );
  process.exit(1);
}
const region = src.slice(b, e);
const fnStart = region.indexOf('function createRefreshScheduler(');
if (fnStart < 0) {
  console.error(
    'FAIL: no `function createRefreshScheduler(` between the markers.',
  );
  process.exit(1);
}
const fnSource = region.slice(fnStart, region.lastIndexOf('}') + 1);
const createRefreshScheduler = new Function(
  `${fnSource}\nreturn createRefreshScheduler;`,
)();

// --- Test harness ----------------------------------------------------------
let failures = 0;
function check(cond, msg) {
  if (!cond) {
    failures++;
    console.error(`FAIL: ${msg}`);
  }
}

// Let promise chains (.then/.finally on already-settled promises) run to
// completion. setImmediate runs after the microtask queue drains, so one
// round is enough for arbitrarily long already-settled chains; do a few to
// be safe against future chain reshaping.
async function drain() {
  for (let i = 0; i < 4; i++) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

// Deterministic manual timers. advance(ms) fires due callbacks in order,
// draining promise chains after each so a callback's .finally reschedules
// before the next timer is considered (matching real event-loop behavior).
function makeFakeTimers() {
  let now = 0;
  let nextId = 1;
  const timers = new Map(); // id -> { fn, at }
  const delays = []; // every delay ever passed to setTimeout
  return {
    setTimeout(fn, ms) {
      const id = nextId++;
      timers.set(id, { fn, at: now + ms });
      delays.push(ms);
      return id;
    },
    clearTimeout(id) {
      timers.delete(id);
    },
    async advance(ms) {
      const target = now + ms;
      for (;;) {
        let dueId = null;
        let dueAt = Infinity;
        for (const [id, t] of timers) {
          if (t.at <= target && t.at < dueAt) {
            dueAt = t.at;
            dueId = id;
          }
        }
        if (dueId === null) break;
        const { fn } = timers.get(dueId);
        timers.delete(dueId);
        now = dueAt;
        fn();
        await drain();
      }
      now = target;
    },
    pending: () => timers.size,
    delays,
  };
}

// Controllable fake refresh: counts starts and concurrency; each call returns
// a promise resolved only when the test calls settleAll().
function makeFakeRefresh() {
  let concurrent = 0;
  const state = { started: 0, maxConcurrent: 0 };
  let resolvers = [];
  state.refresh = () => {
    state.started++;
    concurrent++;
    if (concurrent > state.maxConcurrent) state.maxConcurrent = concurrent;
    return new Promise((resolve) => {
      resolvers.push(() => {
        concurrent--;
        resolve();
      });
    });
  };
  state.settleAll = async () => {
    const rs = resolvers;
    resolvers = [];
    for (const r of rs) r();
    await drain();
  };
  state.inFlightCount = () => concurrent;
  return state;
}

function makeWorld({ hidden }) {
  const timers = makeFakeTimers();
  const fake = makeFakeRefresh();
  const vis = { hidden };
  const scheduler = createRefreshScheduler({
    setTimeout: timers.setTimeout,
    clearTimeout: timers.clearTimeout,
    refresh: fake.refresh,
    isHidden: () => vis.hidden,
  });
  return { timers, fake, vis, scheduler };
}

const VISIBLE_MS = 5000;
const HIDDEN_MS = 60000;

// --- 1. THE regression: visibility flip during an in-flight timer-triggered
//        refresh must not start a second concurrent refresh chain. -----------
{
  const { timers, fake, vis, scheduler } = makeWorld({ hidden: true });
  scheduler.scheduleRefresh();
  check(timers.pending() === 1, 'regression: one timer pending after start');
  check(
    timers.delays.at(-1) === HIDDEN_MS,
    `regression: hidden tab scheduled at ${HIDDEN_MS}ms, got ${timers.delays.at(-1)}`,
  );

  // Hidden-tab timer fires; the refresh is now in flight and stays there.
  await timers.advance(HIDDEN_MS);
  check(fake.started === 1, 'regression: timer fire started one refresh');
  check(fake.inFlightCount() === 1, 'regression: refresh is in flight');
  check(timers.pending() === 0, 'regression: no timer while refresh runs');

  // Tab becomes visible while that fetch is still in flight. Pre-fix, this
  // clearTimeout()ed a spent timer id and started a SECOND concurrent
  // refresh chain. Post-fix it must only reschedule.
  vis.hidden = false;
  scheduler.onVisibilityChange();
  await drain();
  check(
    fake.started === 1,
    `regression: no second refresh during in-flight (started=${fake.started})`,
  );
  check(
    fake.maxConcurrent === 1,
    `regression: never >1 concurrent refresh (max=${fake.maxConcurrent})`,
  );
  check(
    timers.pending() === 1,
    `regression: visibility flip rescheduled exactly one timer (${timers.pending()})`,
  );
  check(
    timers.delays.at(-1) === VISIBLE_MS,
    'regression: reschedule uses the visible cadence',
  );

  // The in-flight refresh completes; its .finally(scheduleRefresh) must
  // coalesce with the visibility-scheduled timer, not add a second chain.
  await fake.settleAll();
  check(
    timers.pending() === 1,
    `regression: still exactly one pending timer after settle (${timers.pending()})`,
  );

  // And the chain keeps running as a single chain afterwards.
  await timers.advance(VISIBLE_MS);
  check(fake.started === 2, 'regression: single chain continues');
  await fake.settleAll();
  check(timers.pending() === 1, 'regression: one timer after next cycle');
  check(fake.maxConcurrent === 1, 'regression: concurrency stayed 1 end-to-end');
}

// --- 2. Cadence: hidden 60s, visible 5s ------------------------------------
{
  const { timers, fake, vis, scheduler } = makeWorld({ hidden: false });
  scheduler.scheduleRefresh();
  check(
    timers.delays.at(-1) === VISIBLE_MS,
    `cadence: visible tab polls at ${VISIBLE_MS}ms, got ${timers.delays.at(-1)}`,
  );

  // Nothing fires early.
  await timers.advance(VISIBLE_MS - 1);
  check(fake.started === 0, 'cadence: no refresh before the visible interval');
  await timers.advance(1);
  check(fake.started === 1, 'cadence: refresh fires at the visible interval');

  // Tab goes hidden; the post-refresh reschedule must use the hidden cadence.
  vis.hidden = true;
  await fake.settleAll();
  check(
    timers.delays.at(-1) === HIDDEN_MS,
    `cadence: hidden tab reschedules at ${HIDDEN_MS}ms, got ${timers.delays.at(-1)}`,
  );
  await timers.advance(HIDDEN_MS - 1);
  check(fake.started === 1, 'cadence: no refresh before the hidden interval');
  await timers.advance(1);
  check(fake.started === 2, 'cadence: refresh fires at the hidden interval');
  await fake.settleAll();
}

// --- 3. visibilitychange -> visible while idle refreshes immediately;
//        while hidden it is a no-op. ----------------------------------------
{
  const { timers, fake, vis, scheduler } = makeWorld({ hidden: true });
  scheduler.scheduleRefresh();

  // Going (or staying) hidden must not trigger anything.
  scheduler.onVisibilityChange();
  await drain();
  check(fake.started === 0, 'visible-flip: hidden visibilitychange is a no-op');
  check(timers.pending() === 1, 'visible-flip: hidden keeps the pending timer');

  // Becoming visible while idle refreshes immediately, without advancing time.
  vis.hidden = false;
  scheduler.onVisibilityChange();
  await drain();
  check(fake.started === 1, 'visible-flip: idle tab refreshes immediately');
  check(
    timers.pending() === 0,
    'visible-flip: stale hidden timer was cancelled',
  );
  await fake.settleAll();
  check(
    timers.pending() === 1,
    'visible-flip: chain rescheduled after immediate refresh',
  );
  check(
    timers.delays.at(-1) === VISIBLE_MS,
    'visible-flip: resumes at the visible cadence',
  );
  check(fake.maxConcurrent === 1, 'visible-flip: concurrency stayed 1');
}

// --- 4. Chain never forks: interleave timer fires and visibility toggles;
//        exactly one pending timer at every quiescent point, never >1
//        concurrent refresh. -------------------------------------------------
{
  const { timers, fake, vis, scheduler } = makeWorld({ hidden: false });
  scheduler.scheduleRefresh();
  for (let round = 0; round < 6; round++) {
    // Fire the pending timer; refresh goes in flight.
    await timers.advance(vis.hidden ? HIDDEN_MS : VISIBLE_MS);
    // Toggle visibility (both directions) while the fetch is in flight —
    // the historical fork window.
    vis.hidden = !vis.hidden;
    scheduler.onVisibilityChange();
    await drain();
    vis.hidden = !vis.hidden;
    scheduler.onVisibilityChange();
    await drain();
    check(
      fake.maxConcurrent === 1,
      `no-fork: round ${round}: >1 concurrent refresh (${fake.maxConcurrent})`,
    );
    await fake.settleAll();
    check(
      timers.pending() === 1,
      `no-fork: round ${round}: expected exactly 1 pending timer, got ${timers.pending()}`,
    );
    check(
      fake.inFlightCount() === 0,
      `no-fork: round ${round}: refreshes all settled`,
    );
  }
  // 6 timer fires + at most one immediate visible-flip refresh per round.
  check(
    fake.started >= 6 && fake.started <= 18,
    `no-fork: sane total refresh count (${fake.started})`,
  );
}

if (failures > 0) {
  console.error(`\ncreateRefreshScheduler: ${failures} check(s) FAILED`);
  process.exit(1);
}
console.log('createRefreshScheduler: all checks passed');
