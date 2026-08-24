// Executable unit test for the permission-event WebSocket helpers in
// `path_handlers/assets/shell_bridge.js` (issue #5213).
//
// The permission channel used to be an EventSource. Every open Freenet tab
// held one for the tab's whole life, and every Freenet app is served from the
// SAME origin, so six tabs consumed the browser's ~6 HTTP/1.1 connections per
// origin and a seventh tab's own document request queued forever with no error
// surfaced. Moving the channel to a WebSocket takes it out of that budget
// (browsers pool WebSockets separately, ~255/profile in Chrome, 200 in
// Firefox).
//
// A WebSocket does NOT auto-reconnect the way EventSource does, so the shell
// now owns reconnection. That new logic — the backoff curve, the jitter, the
// URL scheme, and the envelope dispatch — is what this file exercises.
//
// Same extract-verbatim-between-markers technique as
// shell_bridge_reload.test.mjs. Run via `npm test` in crates/core/src/server
// (wired into the lint-assets CI job). Exits non-zero on any mismatch.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'path_handlers/assets/shell_bridge.js');
const src = readFileSync(assetPath, 'utf8');

function extractFrom(begin, end, needle) {
  const b = src.indexOf(begin);
  const e = src.indexOf(end);
  if (b < 0 || e < 0 || e < b) {
    console.error(
      `FAIL: could not find ${begin}/${end} markers in ${assetPath}. The ` +
        'code must stay bracketed by those markers so this test can extract it.',
    );
    process.exit(1);
  }
  const region = src.slice(b, e);
  const fnStart = region.indexOf(needle);
  if (fnStart < 0) {
    console.error(`FAIL: no \`${needle}\` between the ${begin} markers.`);
    process.exit(1);
  }
  return region.slice(fnStart);
}

const { permSocketUrl, nextPermReconnectDelay, permReconnectJitter, permEventAction } =
  new Function(
    `${extractFrom('perm-ws-decisions:BEGIN', 'perm-ws-decisions:END', 'function permSocketUrl(')}\n` +
      'return { permSocketUrl, nextPermReconnectDelay, permReconnectJitter, permEventAction };',
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

// 1. The regression itself. If this ever fails, #5213 is back: a held-open
//    HTTP request per tab exhausts the browser's per-origin connection budget
//    and hangs every Freenet tab past the sixth. Pinned here as well as in the
//    Rust guard so it fails in whichever suite runs first.
//
//    SCOPE: this rules out the one spelling that actually shipped. A streamed
//    fetch() or a long-poll would break the same invariant and still pass, so
//    it is a fast signal, not the guard. The invariant itself is checked in a
//    real browser by
//    crates/core/tests/playwright/tests/connection-exhaustion.spec.ts.
{
  check('shell opens no EventSource (#5213)', !src.includes('EventSource('));
  check('shell opens the permission WebSocket', src.includes('/permission/events/ws'));
}

// 2. Scheme derivation. A TLS-served shell must upgrade to wss, or the
//    browser's mixed-content rule blocks the socket and every tab silently
//    falls back to 3s polling.
{
  check(
    'http shell -> ws',
    permSocketUrl({ protocol: 'http:', host: '127.0.0.1:7509' }) ===
      'ws://127.0.0.1:7509/permission/events/ws',
  );
  check(
    'https shell -> wss',
    permSocketUrl({ protocol: 'https:', host: 'node.example:443' }) ===
      'wss://node.example:443/permission/events/ws',
  );
  check(
    'host carries the port through',
    permSocketUrl({ protocol: 'http:', host: 'localhost:1234' }).includes('localhost:1234'),
  );
}

// 3. Backoff curve: doubles, then pins at the ceiling. Without the ceiling a
//    long outage would push the retry interval to absurd values and a tab
//    would take minutes to recover after the node came back.
{
  check('1s -> 2s', nextPermReconnectDelay(1000, 30000) === 2000);
  check('2s -> 4s', nextPermReconnectDelay(2000, 30000) === 4000);
  check('16s -> 30s (clamped, not 32s)', nextPermReconnectDelay(16000, 30000) === 30000);
  check('at the ceiling it stays there', nextPermReconnectDelay(30000, 30000) === 30000);
  check('never exceeds the ceiling', nextPermReconnectDelay(1e9, 30000) === 30000);
}

// 4. Jitter spread. Every tab in the browser reconnects off the same node
//    restart; without jitter they retry in lockstep and collide against the
//    server's subscriber cap. Bounds must be +/-20% and must straddle 1.0 so
//    the jitter can both delay and advance.
{
  check('rand=0 -> 80% of the delay', permReconnectJitter(1000, 0) === 800);
  check('rand=1 -> 120% of the delay', Math.abs(permReconnectJitter(1000, 1) - 1200) < 1e-9);
  check('rand=0.5 -> the nominal delay', Math.abs(permReconnectJitter(1000, 0.5) - 1000) < 1e-9);
  // Epsilon because 0.8 + 0.4 is 1.2000000000000002 in IEEE-754, so the top
  // of the band overshoots by ~1e-12. The bound under test is the +/-20%
  // policy, not float exactness.
  const EPS = 1e-6;
  let allInBand = true;
  for (let i = 0; i <= 100; i++) {
    const v = permReconnectJitter(5000, i / 100);
    if (v < 4000 - EPS || v > 6000 + EPS) allInBand = false;
  }
  check('every rand in [0,1] lands within +/-20%', allInBand);
}

// 5. Envelope dispatch. `data` is delegate-controlled, so anything without a
//    string nonce must be dropped rather than reaching the card renderer.
{
  check(
    'prompt_added with a nonce -> add',
    permEventAction({ event: 'prompt_added', data: { nonce: 'n1' } }).action === 'add',
  );
  check(
    'prompt_added carries the nonce through',
    permEventAction({ event: 'prompt_added', data: { nonce: 'n1' } }).nonce === 'n1',
  );
  check(
    'prompt_removed with a nonce -> remove',
    permEventAction({ event: 'prompt_removed', data: { nonce: 'n2' } }).action === 'remove',
  );
  check('resync -> resync', permEventAction({ event: 'resync', data: {} }).action === 'resync');
  check(
    'resync needs no data payload',
    permEventAction({ event: 'resync' }).action === 'resync',
  );

  // Malformed / hostile shapes must all collapse to `ignore`.
  const ignored = [
    ['null envelope', null],
    ['undefined envelope', undefined],
    ['no event field', { data: { nonce: 'n' } }],
    ['non-string event', { event: 42, data: { nonce: 'n' } }],
    ['unknown event name', { event: 'prompt_exploded', data: { nonce: 'n' } }],
    ['added with no data', { event: 'prompt_added' }],
    ['added with null data', { event: 'prompt_added', data: null }],
    ['added with no nonce', { event: 'prompt_added', data: { message: 'hi' } }],
    ['added with a non-string nonce', { event: 'prompt_added', data: { nonce: 7 } }],
    ['removed with a non-string nonce', { event: 'prompt_removed', data: { nonce: {} } }],
  ];
  for (const [name, envelope] of ignored) {
    check(`ignores: ${name}`, permEventAction(envelope).action === 'ignore');
  }
}

// 6. The STATEFUL half: connection lifecycle, fallback-poll transitions and
//    reconnect backoff. Sections 2-5 cover pure helpers; everything that
//    actually decides whether a tab recovers from a node restart lives here.
//    Without this, deleting the backoff reset or the poll-on-close still
//    passed every test while leaving every tab degraded.
//
//    The region is run against stubbed `WebSocket`, timers and `fetch`, so no
//    real IO happens and time is advanced by hand.
{
  function buildMachine() {
    const calls = {
      polls: 0,
      stops: 0,
      reconciles: 0,
      sockets: [],
      shown: [],
      hidden: [],
      warnings: [],
      constructAttempts: 0,
      constructorThrows: false,
      created: [],
    };
    const timers = [];
    let now = 0;

    function setTimeoutStub(fn, ms) {
      const t = { fn, at: now + ms, cancelled: false };
      timers.push(t);
      return t;
    }
    function clearTimeoutStub(t) {
      if (t) t.cancelled = true;
    }
    function advance(ms) {
      now += ms;
      // Copy: a fired timer may schedule another.
      for (const t of timers.slice()) {
        if (!t.cancelled && t.at <= now) {
          t.cancelled = true;
          t.fn();
        }
      }
    }

    class FakeWebSocket {
      constructor(url) {
        calls.constructAttempts++;
        // A constructor that throws is the one path that reaches
        // schedulePermReconnect WITHOUT going through onclose's identity
        // guard, which is what makes the handle dedup observable on its own.
        if (calls.constructorThrows) throw new Error('blocked scheme');
        this.url = url;
        this.closed = false;
        calls.sockets.push(this);
      }
      open() {
        if (this.onopen) this.onopen();
      }
      close() {
        this.closed = true;
        if (this.onclose) this.onclose();
      }
      // Drive a real inbound frame. Without this the dispatch code was LOADED
      // but never RUN: inverting `prompt_added` to hide the card left the whole
      // suite green.
      message(obj) {
        if (this.onmessage) this.onmessage({ data: JSON.stringify(obj) });
      }
    }

    // Real interval bookkeeping, not a stub counter. `startFallbackPoll`'s
    // idempotency guard is load-bearing now that openPermSocket AND onclose
    // both call it; only counting LIVE intervals can express the leak that
    // dropping the guard would cause.
    let nextIntervalId = 1;
    const liveIntervals = new Map();
    function setIntervalStub(fn, ms) {
      const id = nextIntervalId++;
      liveIntervals.set(id, { fn, ms });
      calls.polls++;
      return id;
    }
    function clearIntervalStub(id) {
      if (liveIntervals.delete(id)) calls.stops++;
    }

    const src = extractFrom(
      'perm-ws-machine:BEGIN',
      'perm-ws-machine:END',
      'var fallbackPollHandle',
    );
    const factory = new Function(
      'WebSocket',
      'setTimeout',
      'clearTimeout',
      'setInterval',
      'clearInterval',
      'Math',
      'location',
      'console',
      'calls',
      'overlayCards',
      `
      function reconcileFromPending() { calls.reconciles++; }
      function showCard(nonce) { calls.shown.push(nonce); overlayCards[nonce] = {}; }
      function hideCard(nonce) { calls.hidden.push(nonce); delete overlayCards[nonce]; }
      function createCard(p) { calls.created.push(p); return { nonce: p && p.nonce }; }
      ${src}
      return {
        openPermSocket: openPermSocket,
        state: function () {
          return {
            delay: permReconnectDelay,
            socket: permSocket,
            pollHandle: fallbackPollHandle,
          };
        },
      };
      `,
    );
    // Deterministic jitter at the midpoint, so scheduled delays are nominal.
    // Built explicitly rather than spread from `Math`, whose methods are
    // non-enumerable and would silently not be copied.
    const mathStub = { min: Math.min, max: Math.max, random: () => 0.5 };
    const overlayCards = {};
    const machine = factory(
      FakeWebSocket,
      setTimeoutStub,
      clearTimeoutStub,
      setIntervalStub,
      clearIntervalStub,
      mathStub,
      { protocol: 'http:', host: '127.0.0.1:7509' },
      { warn: (...a) => calls.warnings.push(a.join(' ')) },
      calls,
      overlayCards,
    );
    return { machine, calls, advance, timers, overlayCards, liveIntervals };
  }

  // 6a. Opening starts the fallback poll immediately, so a HANGING handshake
  //     (a proxy swallowing Upgrade:) still shows prompts. This is the gap
  //     that existed when the poll started only from `onclose`.
  {
    const { machine, calls } = buildMachine();
    machine.openPermSocket();
    check('opening starts the fallback poll before the socket resolves', calls.polls === 1);
    check('a socket was constructed', calls.sockets.length === 1);
    check(
      'the socket URL is the ws endpoint',
      calls.sockets[0].url === 'ws://127.0.0.1:7509/permission/events/ws',
    );
  }

  // 6b. A successful open stops the poll and reconciles.
  {
    const { machine, calls } = buildMachine();
    machine.openPermSocket();
    const reconcilesBeforeOpen = calls.reconciles;
    calls.sockets[0].open();
    check('open stops the fallback poll', calls.stops === 1);
    // A DELTA, not `>= 1`: startFallbackPoll already contributed one reconcile
    // before the handshake resolved, so `>= 1` cannot tell whether onopen
    // reconciled at all. Deleting the onopen reconcile passed under `>= 1` —
    // the same vacuity family as the old `> 1000` backoff assertion.
    check(
      'open reconciles so nothing is missed across the gap',
      calls.reconciles === reconcilesBeforeOpen + 1,
    );
  }

  // 6c. Close restarts the poll and schedules exactly one reconnect.
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    calls.sockets[0].open();
    calls.sockets[0].close();
    check('close restarts the fallback poll', calls.polls === 2);
    advance(5000);
    check('close schedules exactly one reconnect', calls.sockets.length === 2);
  }

  // 6d. The backoff must NOT reset the instant the socket opens. A peer that
  //     accepts the upgrade then drops it immediately would otherwise pin
  //     retries at ~1s forever with no growth — the same shape of silent
  //     unbounded loop as #5213 itself.
  //
  //     The assertion must check COMPOUND growth, not merely `> 1000`. With
  //     an immediate reset each cycle is reset-to-1s then double-to-2s, so a
  //     `> 1000` check passes under the very bug it is meant to catch — this
  //     test was vacuous until a mutation run caught it. Four cycles give
  //     1s→2s→4s→8s→16s when correct, versus a flat 2s when broken.
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    for (let i = 0; i < 4; i++) {
      const sock = calls.sockets[calls.sockets.length - 1];
      sock.open();
      sock.close(); // dies immediately, well inside PERM_STABLE_AFTER
      advance(60000);
    }
    check(
      'accept-then-immediately-close compounds the backoff (not pinned at ~1Hz)',
      machine.state().delay >= 8000,
    );
  }

  // 6e. A socket that stays up past the stability window DOES reset it, so a
  //     genuine recovery is fast rather than stuck at the 30s ceiling.
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    // Drive the delay up first.
    for (let i = 0; i < 3; i++) {
      const s = calls.sockets[calls.sockets.length - 1];
      s.open();
      s.close();
      advance(60000);
    }
    const grown = machine.state().delay;
    check('precondition: backoff grew', grown > 1000);
    calls.sockets[calls.sockets.length - 1].open();
    advance(15000); // past PERM_STABLE_AFTER
    check('a stable socket resets the backoff to 1s', machine.state().delay === 1000);
  }

  // 6g. INBOUND DISPATCH. Proven missing by mutation: inverting `prompt_added`
  //     to hide the card, and disabling the `resync -> reconcileFromPending`
  //     branch, both left the entire suite green. Section 5 proves
  //     `permEventAction` CLASSIFIES an envelope; nothing drove the wire
  //     between that classification and the DOM, which is the code this change
  //     wrote.
  {
    const { machine, calls, overlayCards } = buildMachine();
    machine.openPermSocket();
    const sock = calls.sockets[0];
    sock.open();
    const reconcilesAfterOpen = calls.reconciles;

    sock.message({ event: 'prompt_added', data: { nonce: 'n1' } });
    check('an inbound prompt_added renders the card', overlayCards.n1 !== undefined);
    check('prompt_added does not hide anything', calls.hidden.length === 0);

    sock.message({ event: 'prompt_removed', data: { nonce: 'n1' } });
    check('an inbound prompt_removed dismisses the card', overlayCards.n1 === undefined);
    check('prompt_removed hid the right nonce', calls.hidden[0] === 'n1');

    sock.message({ event: 'resync' });
    check(
      'an inbound resync re-bootstraps from /permission/pending',
      calls.reconciles === reconcilesAfterOpen + 1,
    );

    // Delegate-controlled payloads must not reach the renderer.
    const shownBefore = calls.shown.length;
    sock.message({ event: 'prompt_added', data: { nonce: 7 } });
    sock.message({ event: 'prompt_exploded', data: { nonce: 'n2' } });
    check('a malformed inbound frame renders nothing', calls.shown.length === shownBefore);

    // A non-JSON frame must not throw out of onmessage.
    let threw = false;
    try {
      sock.onmessage({ data: 'not json' });
    } catch (e) {
      threw = true;
    }
    check('a non-JSON frame is swallowed, not thrown', threw === false);
  }

  // 6t. The FIRST retry must use the current delay, not the advanced one.
  //     `permReconnectDelay` is advanced after the timer is armed. NOTE the
  //     mutation that actually produces the bug is moving that advance ABOVE
  //     `var jittered = ...`; merely swapping the advance with the setTimeout
  //     is a no-op, because `jittered` is already computed from the pre-advance
  //     value. Naming the no-op here would send a future editor to a mutation
  //     that stays green and invite them to delete this pin as vacuous.
  {
    const { machine, calls, advance, timers } = buildMachine();
    machine.openPermSocket();
    calls.sockets[0].close();
    const armed = timers.filter((t) => !t.cancelled);
    check(
      'the first reconnect is armed at ~1s, not the doubled delay',
      armed.length === 1 && armed[0].at === 1000,
    );
    advance(1000);
    check('and it fires at that time', calls.sockets.length === 2);
  }

  // 6u. A pending reconnect must SURVIVE a new open. Two reviewers disagreed
  //     about clearing it; the tiebreak is that a hung upgrade (no open, no
  //     close — the common WebSocket-through-proxy failure) leaves nothing else
  //     to retry, so clearing it strands the tab on the 3s poll permanently,
  //     whereas leaving it costs at most one wasted cycle that self-heals.
  //     Pin the behaviour so the clear cannot come back unnoticed.
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    calls.sockets[0].close(); // arms a reconnect
    const beforeOpen = calls.sockets.length;
    machine.openPermSocket(); // the hypothetical visibilitychange trigger
    const hung = calls.sockets[calls.sockets.length - 1]; // never opens, never closes
    advance(60000);
    check(
      'a hung open still gets retried by the armed reconnect',
      calls.sockets.length > beforeOpen + 1,
    );
    check('the hung socket was retired', hung.closed === true);
  }

  // 6r. A repeated prompt_added for a nonce already on screen must be ignored.
  //     Without the guard, showCard overwrites overlayCards[nonce] and the
  //     first card element is orphaned in the DOM forever — a leak a hostile
  //     or merely retrying delegate can drive.
  {
    const { machine, calls, overlayCards } = buildMachine();
    machine.openPermSocket();
    const sock = calls.sockets[0];
    sock.open();
    sock.message({ event: 'prompt_added', data: { nonce: 'dup' } });
    sock.message({ event: 'prompt_added', data: { nonce: 'dup' } });
    check('a repeated prompt_added is ignored, not re-rendered', calls.shown.length === 1);
    check('the card is still present after the duplicate', overlayCards.dup !== undefined);
  }

  // 6s. The card must be built from the delegate's payload, not just its nonce.
  //     Passing only the nonce would render a prompt with no message, no
  //     button labels and no delegate hash — a security prompt the user cannot
  //     evaluate. 6g's stub reads only `nonce`, so it could not see this.
  {
    const { machine, calls } = buildMachine();
    machine.openPermSocket();
    const sock = calls.sockets[0];
    sock.open();
    sock.message({
      event: 'prompt_added',
      data: { nonce: 'n9', message: 'Allow?', labels: ['yes', 'no'] },
    });
    check(
      'createCard receives the full payload, not just the nonce',
      calls.created.length === 1 && calls.created[0].message === 'Allow?',
    );
  }

  // 6h. The fallback poll must never accumulate intervals. openPermSocket
  //     starts it AND onclose starts it again, so every retry cycle calls
  //     startFallbackPoll twice; without its idempotency guard each cycle
  //     leaks a live interval hammering /permission/pending forever. Counting
  //     LIVE intervals is the point — a stub counter cannot express a leak.
  {
    const { machine, calls, advance, liveIntervals } = buildMachine();
    machine.openPermSocket();
    for (let i = 0; i < 4; i++) {
      const s2 = calls.sockets[calls.sockets.length - 1];
      s2.close();
      advance(60000);
    }
    check(
      'repeated reconnect cycles never accumulate poll intervals',
      liveIntervals.size <= 1,
    );
    const live = calls.sockets[calls.sockets.length - 1];
    live.open();
    check('a successful open leaves no poll interval running', liveIntervals.size === 0);
  }

  // 6i. The socket-identity guard, on its own. 6f cannot test this: the
  //     identity guard and schedulePermReconnect's handle dedup each
  //     short-circuit before the other is reached, so 6f stayed green under
  //     deletion of EITHER. Two live sockets is the only shape that isolates
  //     the identity guard.
  {
    const { machine, calls } = buildMachine();
    machine.openPermSocket();
    const a = calls.sockets[0];
    a.open();
    // A second open (the `visibilitychange`/`online` trigger the guard's
    // comment anticipates) retires A and installs B.
    machine.openPermSocket();
    const b = calls.sockets[calls.sockets.length - 1];
    check('a second open installs a new socket', b !== a);
    b.open();
    // Now the STALE socket closes. It must not disturb the live one.
    a.close();
    check(
      "a stale socket's close does not orphan the live socket",
      machine.state().socket === b,
    );
  }

  // 6j. schedulePermReconnect's handle dedup, on its own — the other half 6f
  //     could not isolate, because onclose's identity guard short-circuits
  //     before the dedup is ever reached, so 6f stayed green under deletion of
  //     EITHER guard. The constructor-throw path is the one route to
  //     schedulePermReconnect that does NOT pass through onclose, so two failed
  //     opens reach the dedup twice with nothing else in the way.
  {
    const { machine, calls, advance } = buildMachine();
    calls.constructorThrows = true;
    machine.openPermSocket(); // attempt 1, schedules
    machine.openPermSocket(); // attempt 2, must be deduped
    advance(60000); // fires whatever was pending -> attempt 3
    check(
      'two failed opens schedule ONE reconnect, not two',
      calls.constructAttempts === 3,
    );
  }

  // 6k. The degraded-tab warning must actually fire. It is the only signal a
  //     user or developer gets that a tab is stuck on the 3s poll.
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    for (let i = 0; i < 6; i++) {
      calls.sockets[calls.sockets.length - 1].close();
      advance(60000);
    }
    check('a persistently failing socket warns', calls.warnings.length > 0);
  }

  // 6l. Constants and bootstrap, all previously unpinned. Each of these is a
  //     magnitude the code's own comments rely on, and each could be changed to
  //     an absurd value with every other test still green.
  {
    const { machine, calls, liveIntervals } = buildMachine();
    machine.openPermSocket();
    const iv = [...liveIntervals.values()][0];
    // 3s, because auto-deny is 60s: any interval near or past that makes the
    // fallback useless while every other stated invariant still holds.
    check('the fallback poll interval is 3s', iv && iv.ms === 3000);
    // The immediate bootstrap. 6a asserts an interval EXISTS; that is not the
    // same as having fetched anything, and deleting the immediate reconcile
    // passed. This is the documented "no separate bootstrap call" path.
    check('opening reconciles immediately, not only on the next tick', calls.reconciles === 1);
  }

  // 6m. The reconnect ceiling. Section 3 passes the ceiling in as a literal
  //     argument, so the CONSTANT is never read by any test; raising it to
  //     300000 changed nothing.
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    for (let i = 0; i < 12; i++) {
      calls.sockets[calls.sockets.length - 1].close();
      advance(600000);
    }
    check('the reconnect delay is capped at 30s', machine.state().delay === 30000);
  }

  // 6n. PERM_STABLE_AFTER's MAGNITUDE. 6d/6e close synchronously after open
  //     with zero elapsed time, so any window > 0 passed — the constant could
  //     be 1ms and the anti-flap protection would be gone while 6d still
  //     asserted growth.
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    // Grow the backoff first.
    for (let i = 0; i < 3; i++) {
      calls.sockets[calls.sockets.length - 1].close();
      advance(60000);
    }
    const grown = machine.state().delay;
    check('precondition: backoff grew before the short-lived open', grown > 1000);
    // A socket that lives 2s is NOT stable; the backoff must not reset.
    calls.sockets[calls.sockets.length - 1].open();
    advance(2000);
    check(
      'a 2s-lived socket does not count as stable (window is not ~0)',
      machine.state().delay === grown,
    );
  }

  // 6o. The retired socket must actually be CLOSED, not merely dropped.
  //     6i asserts which socket is installed; deleting the whole retire block
  //     passed. The consequence is a leaked server-side subscriber slot
  //     against MAX_PERMISSION_SUBSCRIBERS until its send timeout — the exact
  //     resource the Rust side spends two tests defending.
  {
    const { machine, calls } = buildMachine();
    machine.openPermSocket();
    const a = calls.sockets[0];
    a.open();
    machine.openPermSocket();
    check('opening a second socket closes the first', a.closed === true);
  }

  // 6p. The stale STABILITY timer must be cleared when a new socket opens.
  //     6i drives two live sockets but never advances time, so A's timer never
  //     fired and deleting the clear passed. If it survives, A's timer resets
  //     the backoff for a dead socket and nulls B's handle.
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    for (let i = 0; i < 3; i++) {
      calls.sockets[calls.sockets.length - 1].close();
      advance(60000);
    }
    const grown = machine.state().delay;
    calls.sockets[calls.sockets.length - 1].open(); // A opens, arms its timer
    advance(9000); // still inside PERM_STABLE_AFTER
    machine.openPermSocket(); // B replaces A; A's timer must be cleared
    advance(5000); // A's timer would have fired at 10s
    check(
      "a replaced socket's stability timer cannot reset the backoff",
      machine.state().delay === grown,
    );
  }

  // 6q. The failure counter must reset WITH the backoff, not on bare open.
  //     6k only closes sockets it never opened, so the reset location was
  //     unreachable from it and moving it back to onopen — the documented old
  //     bug — passed. Accept-then-drop is the case that distinguishes them.
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    for (let i = 0; i < 6; i++) {
      const sk = calls.sockets[calls.sockets.length - 1];
      sk.open(); // accepted...
      sk.close(); // ...then dropped immediately, never reaching stability
      advance(60000);
    }
    check('accept-then-drop still reaches the warning threshold', calls.warnings.length > 0);
    // And the warning must not become its own noise, which the code claims.
    check('the warning fires once, not on every attempt', calls.warnings.length === 1);
  }

  // 6f. No double-scheduling: a second close while a reconnect is pending must
  //     not queue a second one (that would halve the effective backoff).
  {
    const { machine, calls, advance } = buildMachine();
    machine.openPermSocket();
    const sock = calls.sockets[0];
    sock.open();
    sock.close();
    sock.close(); // stale close on an already-cleared socket
    advance(5000);
    check('a duplicate close does not double-schedule', calls.sockets.length === 2);
  }
}

// 7. `reconcileFromPending` must never destroy a prompt it could not have
//    known about. Found in review of PR #5269, and it is the worst failure
//    mode in this file: the card is removed from the DOM with a healthy socket
//    and nothing to re-add it, so the prompt auto-denies at the server timeout
//    and the USER NEVER SEES IT. Reachable on any node restart —
//
//      1. socket closes, the fallback poll issues fetch F1 (slow, node is
//         restarting)
//      2. socket reconnects; stopFallbackPoll clears the interval but cannot
//         cancel F1
//      3. prompt X arrives over the fresh socket -> showCard(X)
//      4. F1 lands carrying the PRE-restart list, which has no X
//
//    A generation counter does not fix this: F1 is the newest response at the
//    moment it lands. Only comparing each card's `_fnShownAt` against when the
//    request was ISSUED does.
{
  const reconcileSrc = extractFrom(
    'perm-reconcile:BEGIN',
    'perm-reconcile:END',
    'function reconcileFromPending(',
  );
  const removalMemorySrc = extractFrom(
    'perm-removal-memory:BEGIN',
    'perm-removal-memory:END',
    'var REMOVAL_MEMORY_MS',
  );

  // Free variables of the extracted region, supplied as parameters so the real
  // source runs unmodified against stubs.
  const makeReconcile = new Function(
    'permNow',
    'fetch',
    'overlayCards',
    'showCard',
    'createCard',
    'hideCard',
    'ctl',
    // The REAL removal-memory block, extracted and run — not a harness copy.
    // A duplicate here is how four mutations to the shipped guard (delete it,
    // never stamp, prune instantly, revert the in-flight fix wholesale) all
    // passed: the tests were asserting properties of the stand-in.
    `${removalMemorySrc}
     ${reconcileSrc}
     ctl.noteRemoval = function (t, nonce) {
       var save = ctl.clock.t;
       ctl.clock.t = t;
       noteRemovalObserved(nonce || 'X');
       ctl.clock.t = save;
     };
     ctl.inFlight = function () { return reconcilesInFlight.slice(); };
     ctl.records = function () { return Object.keys(removalObservedAt).length; };
     ctl.observed = function (nonce, since) { return removalObservedSince(nonce, since); };
     return reconcileFromPending;`,
  );

  function build(pending) {
    const clock = { t: 1000 };
    const permNow = () => clock.t;
    const overlayCards = {};
    const hidden = [];
    const ctl = {};
    ctl.clock = clock; // the real noteRemovalObserved reads permNow()
    let release = null;
    const fetchStub = () =>
      new Promise((res) => {
        release = () => res({ json: () => Promise.resolve(pending) });
      });
    // Mirrors the real showCard's stamping. Pinned against the real source
    // separately below, since showCard itself lives outside the region.
    const showCard = (nonce, card) => {
      card._fnShownAt = permNow();
      overlayCards[nonce] = card;
    };
    const createCard = (p) => ({ nonce: p.nonce });
    const hideCard = (nonce) => {
      delete overlayCards[nonce];
      hidden.push(nonce);
    };
    return {
      clock,
      overlayCards,
      hidden,
      showCard,
      createCard,
      ctl,
      reconcile: makeReconcile(
        permNow,
        fetchStub,
        overlayCards,
        showCard,
        createCard,
        hideCard,
        ctl,
      ),
      // `elapsed` ADVANCES THE CLOCK before the response lands. Without it the
      // fixture froze time across the fetch, so `permNow()` at response time
      // equalled `issuedAt` and the two readings this whole section exists to
      // distinguish were numerically identical — mutating the hide pass to
      // compare against response time instead of request time passed every
      // test in the section while destroying, in a real browser, every card
      // raised during an in-flight fetch. That is the #5269 bug itself.
      settle: async (elapsed = 50) => {
        clock.t += elapsed;
        release();
        // Drain the chain: fetch -> r.json() -> handler -> catch ->
        // deregister. Each is its own microtask hop, so under-awaiting here
        // silently skips the tail of the chain (which is how the
        // deregistration checks first failed).
        for (let i = 0; i < 8; i++) await Promise.resolve();
      },
    };
  }

  // Variant of build() whose fetch REJECTS, for the deregistration path.
  function buildRejecting() {
    const clock = { t: 1000 };
    const permNow = () => clock.t;
    const overlayCards = {};
    const hidden = [];
    const ctl = {};
    ctl.clock = clock; // the real noteRemovalObserved reads permNow()
    let release = null;
    const fetchStub = () =>
      new Promise((_res, rej) => {
        release = () => rej(new Error('network down'));
      });
    const showCard = (nonce, card) => {
      card._fnShownAt = permNow();
      overlayCards[nonce] = card;
    };
    const createCard = (p) => ({ nonce: p.nonce });
    const hideCard = (nonce) => {
      delete overlayCards[nonce];
      hidden.push(nonce);
    };
    return {
      clock,
      overlayCards,
      hidden,
      ctl,
      reconcile: makeReconcile(
        permNow,
        fetchStub,
        overlayCards,
        showCard,
        createCard,
        hideCard,
        ctl,
      ),
      settle: async () => {
        release();
        for (let i = 0; i < 8; i++) await Promise.resolve();
      },
    };
  }

  // 7a. The regression itself: a card raised AFTER the request went out must
  //     survive a snapshot that predates it.
  {
    const env = build([]);
    env.reconcile();
    env.clock.t = 1001;
    env.showCard('X', env.createCard({ nonce: 'X' }));
    await env.settle();
    check(
      'a prompt raised while the snapshot was in flight is NOT destroyed',
      env.overlayCards.X !== undefined && env.hidden.length === 0,
    );
  }

  // 7b. The tie. Date.now() is millisecond-granular and the node is on
  //     loopback, so same-millisecond is genuinely ambiguous; it must resolve
  //     toward keeping the card, because a wrongly-kept card is corrected by
  //     the next reconcile whereas a wrongly-hidden one is gone for good.
  //     (With permNow() on performance.now() an exact tie is essentially
  //     unreachable in practice; this pins the direction, not a live case.)
  {
    const env = build([]);
    env.reconcile();
    env.showCard('X', env.createCard({ nonce: 'X' })); // same ms as issuedAt
    await env.settle();
    check(
      'a card shown in the SAME millisecond the request was issued survives',
      env.overlayCards.X !== undefined,
    );
  }

  // 7c. Control. Without this the fix could degenerate into "never hide
  //     anything", and cross-tab dismissal would silently stop working: a
  //     prompt answered in another tab would stay on screen forever.
  {
    const env = build([]);
    env.clock.t = 999;
    env.showCard('X', env.createCard({ nonce: 'X' }));
    env.clock.t = 1000;
    env.reconcile();
    await env.settle();
    check(
      'a card the snapshot COULD have seen, and did not, is still hidden',
      env.overlayCards.X === undefined && env.hidden[0] === 'X',
    );
  }

  // 7d. The additive half of reconciliation still works.
  {
    const env = build([{ nonce: 'Y' }]);
    env.reconcile();
    await env.settle();
    check('a pending prompt not yet on screen is shown', env.overlayCards.Y !== undefined);
  }

  // 7f. The ADD pass needs the mirror of the hide pass's causal check, or a
  //     stale snapshot RESURRECTS a prompt answered after the request went
  //     out. With a healthy socket nothing then removes the ghost, so the user
  //     is left looking at a security prompt for a dead nonce.
  {
    const env = build([{ nonce: 'X' }]);
    env.reconcile(); // issuedAt = 1000
    env.clock.t = 1001;
    env.ctl.noteRemoval(1001, 'X'); // X answered in another tab, after F1 went out
    await env.settle();
    check(
      'a snapshot that predates a known removal does not resurrect the prompt',
      env.overlayCards.X === undefined,
    );
  }

  // 7h. The guard must suppress ONLY the nonce actually removed. A single
  //     shared timestamp meant one removal blocked every add, and on the
  //     `resync` path that loss is PERMANENT: resync exists because the server
  //     dropped prompt_added frames (never resent), the socket is healthy so
  //     the 3s poll is stopped, and reconcile is the only add mechanism left.
  //     An unrelated removal landing inside that fetch window would bury the
  //     dropped prompt until the server auto-denied it.
  //
  //     7f/7g cannot catch this: they use the same nonce for the removal and
  //     the add, so cross-nonce over-suppression passes vacuously there.
  {
    const env = build([{ nonce: 'DROPPED' }]);
    env.reconcile(); // resync's fetch goes out at 1000
    env.clock.t = 1001;
    env.ctl.noteRemoval(1001, 'UNRELATED'); // a different prompt is answered
    await env.settle();
    check(
      "an unrelated nonce's removal does not suppress a dropped prompt (resync)",
      env.overlayCards.DROPPED !== undefined,
    );
  }

  // 7g. Control for 7f. A removal observed BEFORE the request was issued says
  //     nothing about this snapshot, so the add must still happen — otherwise
  //     the guard would degenerate into "never add anything again".
  {
    const env = build([{ nonce: 'X' }]);
    env.ctl.noteRemoval(999, 'X');
    env.clock.t = 1000;
    env.reconcile(); // issuedAt = 1000, strictly after the removal
    await env.settle();
    check(
      'a removal older than the request does not block the add',
      env.overlayCards.X !== undefined,
    );
  }

  // 7i. STEADY STATE — the commonest case of all, and it had no test: a card
  //     on screen AND present in the snapshot must be left alone. 7c covers
  //     absent->hide and 7d covers present->show; without this, deleting the
  //     `seen[p.nonce] = true` marking passes, and every reconcile would hide
  //     every visible card — every 3s on every degraded tab, and on every
  //     resync.
  {
    const env = build([{ nonce: 'KEEP' }]);
    env.clock.t = 900;
    env.showCard('KEEP', env.createCard({ nonce: 'KEEP' }));
    env.clock.t = 1000;
    env.reconcile();
    await env.settle();
    check(
      'a card that is on screen AND in the snapshot is left alone',
      env.overlayCards.KEEP !== undefined && env.hidden.length === 0,
    );
  }

  // 7j. The one-line editing slip: moving the add-pass guard ABOVE the
  //     seen-marking. Skipping the add would then also un-mark the nonce, so
  //     the hide pass below destroys the live card — turning the 7f fix into a
  //     card-destroyer. 7f/7g cannot see it: they run with an empty
  //     overlayCards, so the hide pass has nothing to act on.
  {
    const env = build([{ nonce: 'LIVE' }]);
    env.clock.t = 900;
    env.showCard('LIVE', env.createCard({ nonce: 'LIVE' }));
    env.clock.t = 1000;
    env.reconcile();
    env.clock.t = 1001;
    env.ctl.noteRemoval(1001, 'LIVE'); // makes the add-pass guard fire for LIVE
    await env.settle();
    check(
      'a suppressed add still marks the nonce seen, so the hide pass spares it',
      env.overlayCards.LIVE !== undefined,
    );
  }

  // 7k. The ADD-pass tie, mirroring 7b on the hide side. A removal stamped at
  //     EXACTLY issuedAt is ambiguous; resolve it toward NOT resurrecting,
  //     which is the conservative direction here (showing a dead prompt is the
  //     harm this guard exists to prevent).
  {
    const env = build([{ nonce: 'X' }]);
    env.reconcile(); // issuedAt = 1000
    env.ctl.noteRemoval(1000, 'X'); // exactly the tie
    await env.settle();
    check('an add whose removal ties with issuedAt is suppressed', env.overlayCards.X === undefined);
  }

  // 7l. Snapshot input validation. `/permission/pending` is delegate-influenced
  //     content; a non-array body or an entry with a non-string nonce must not
  //     reach the renderer or corrupt the seen-set.
  {
    const env = build({ not: 'an array' });
    env.clock.t = 900;
    env.showCard('KEEP', env.createCard({ nonce: 'KEEP' }));
    env.clock.t = 1000;
    env.reconcile();
    await env.settle();
    check(
      'a non-array snapshot is ignored and destroys nothing',
      env.overlayCards.KEEP !== undefined && env.hidden.length === 0,
    );
  }
  {
    const env = build([{ nonce: 42 }, { nonce: 'GOOD' }]);
    env.reconcile();
    await env.settle();
    check(
      'a malformed entry is skipped while a valid sibling still renders',
      env.overlayCards.GOOD !== undefined && env.overlayCards[42] === undefined,
    );
  }

  // 7m. In-flight bookkeeping. The removal map is pruned against the OLDEST
  //     outstanding request, so a request must be registered while it runs and
  //     deregistered on EVERY outcome. A leak here pins the prune floor
  //     forever and the map grows without bound; a missing registration lets a
  //     wedged-node fetch (minutes, no timeout) have its removal record pruned
  //     and resurrect an answered prompt on arrival.
  {
    const env = build([]);
    env.reconcile();
    check('a running reconcile is registered in-flight', env.ctl.inFlight().length === 1);
    await env.settle();
    check('and deregistered when it lands', env.ctl.inFlight().length === 0);
  }
  {
    // Non-array body: the early return must still deregister.
    const env = build({ not: 'an array' });
    env.reconcile();
    await env.settle();
    check('a malformed body still deregisters', env.ctl.inFlight().length === 0);
  }
  {
    // A rejected fetch must deregister too — this is the leak case.
    const env = buildRejecting();
    env.reconcile();
    await env.settle();
    check('a FAILED fetch still deregisters', env.ctl.inFlight().length === 0);
  }

  // 7n. PRUNING. Two mutations to the shipped prune survived even after the
  //     real block was extracted, because nothing drove it: dropping
  //     REMOVAL_MEMORY_MS to 0, and reverting to a bare wall-clock window.
  //     Both are the difference between "the guard exists" and "the guard is
  //     still there when the response lands".
  {
    // (a) A record an OLD in-flight request could still need must SURVIVE the
    //     soft window. This is the wedged-node case: /permission/pending has
    //     no timeout, so a fetch can outlive the window, and pruning its record
    //     resurrects the answered prompt on arrival.
    const env = build([]);
    env.reconcile(); // R1 issued at 1000 and left in flight
    env.ctl.noteRemoval(1001, 'X');
    env.clock.t = 1001 + 70000; // well past REMOVAL_MEMORY_MS
    env.ctl.noteRemoval(env.clock.t, 'DRIVE'); // drives the prune
    check(
      'a record an in-flight request still needs survives the soft window',
      env.ctl.observed('X', 1000) === true,
    );
  }
  {
    // (b) With nothing in flight a record still has to live for the window,
    //     or the guard evaporates before any response can use it.
    const env = build([]);
    env.ctl.noteRemoval(1000, 'X');
    env.clock.t = 1100;
    env.ctl.noteRemoval(1100, 'DRIVE');
    check(
      'a fresh record is not pruned immediately',
      env.ctl.observed('X', 1000) === true,
    );
  }
  {
    // (c) The absolute-age override. A request that never resolves must stop
    //     pinning the floor, or the exemption has neither a TTL nor an age
    //     bound and both structures grow for the rest of the session — the
    //     recurring cleanup-exemption bug class this project tracks.
    const env = build([]);
    env.reconcile(); // R1 at 1000, never settled
    env.ctl.noteRemoval(1001, 'X');
    env.clock.t = 1001 + 700000; // past REMOVAL_MEMORY_HARD_MS
    env.ctl.noteRemoval(env.clock.t, 'DRIVE');
    check(
      'a dead in-flight request stops pinning the prune floor',
      env.ctl.inFlight().length === 0,
    );
    check(
      'and records past the absolute bound are collected',
      env.ctl.observed('X', 1000) === false,
    );
  }

  // 7e. The stamp itself. 7a-7c stub showCard, so they would all still pass if
  //     the real showCard stopped recording `_fnShownAt` — every card would
  //     then compare `undefined >= issuedAt` (false) and be hidden, which is
  //     precisely the bug. Bound the scrape to showCard's own body: an
  //     unbounded search would match the stub-mirroring comment above.
  {
    const fnStart = src.indexOf('function showCard(');
    const after = src.indexOf('\n  function ', fnStart + 1);
    const body = fnStart < 0 ? '' : src.slice(fnStart, after < 0 ? undefined : after);
    // 7e-bis. The PRODUCER of the 7f/7g signal. Those tests inject the removal
    // through the harness, so the guard is exercised but nothing that feeds it
    // is: deleting `noteRemovalObserved(nonce)` from hideCard, or moving it
    // BELOW the `if (!card) return;` early return, both left the suite green.
    // The position is load-bearing and argued at length in the source — a
    // `prompt_removed` for a nonce this tab never rendered is still an
    // observed removal — so pin the position, not just the presence. This is
    // the sibling of the showCard pin below; its absence was an asymmetry.
    {
      const fnStart = src.indexOf('function hideCard(');
      const after = src.indexOf('\n  function ', fnStart + 1);
      const body = fnStart < 0 ? '' : src.slice(fnStart, after < 0 ? undefined : after);
      const callAt = body.indexOf('noteRemovalObserved(nonce)');
      const returnAt = body.indexOf('if (!card) return;');
      // And the clock itself. Without this, `permNow` could be rewritten to
      // return Date.now() unconditionally and every check here — including the
      // one literally named "MONOTONIC" — would stay green, re-opening the
      // NTP-step-backwards window the source argues about at length.
      {
        const nStart = src.indexOf('function permNow(');
        const nAfter = src.indexOf('\n  ', src.indexOf('}', nStart));
        const nBody = nStart < 0 ? '' : src.slice(nStart, nAfter < 0 ? undefined : nAfter);
        check(
          'permNow prefers performance.now(), not the wall clock',
          nBody.includes('performance.now()'),
        );
      }

      check(
        'hideCard records the removal, ABOVE its early return',
        callAt >= 0 && returnAt >= 0 && callAt < returnAt,
      );
    }

    check(
      'showCard records _fnShownAt on the MONOTONIC clock, which the comparison depends on',
      body.includes('_fnShownAt = permNow()'),
    );
  }
}

if (failures > 0) {
  console.error(`permission-ws: ${failures} check(s) failed`);
  process.exit(1);
}
console.log('permission-ws: all checks passed');
