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
{
  check('shell opens no EventSource (#5213)', !src.includes('new EventSource('));
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
    const calls = { polls: 0, stops: 0, reconciles: 0, sockets: [] };
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
        this.url = url;
        calls.sockets.push(this);
      }
      open() {
        if (this.onopen) this.onopen();
      }
      close() {
        if (this.onclose) this.onclose();
      }
    }

    const src = extractFrom('perm-ws-machine:BEGIN', 'perm-ws-machine:END', 'var permSocket');
    const factory = new Function(
      'WebSocket',
      'setTimeout',
      'clearTimeout',
      'Math',
      'location',
      'console',
      'calls',
      `
      function startFallbackPoll() { calls.polls++; }
      function stopFallbackPoll() { calls.stops++; }
      function reconcileFromPending() { calls.reconciles++; }
      var overlayCards = {};
      function showCard() {}
      function hideCard() {}
      function createCard() { return {}; }
      ${src}
      return {
        openPermSocket: openPermSocket,
        state: function () {
          return { delay: permReconnectDelay, socket: permSocket };
        },
      };
      `,
    );
    // Deterministic jitter at the midpoint, so scheduled delays are nominal.
    // Built explicitly rather than spread from `Math`, whose methods are
    // non-enumerable and would silently not be copied.
    const mathStub = { min: Math.min, max: Math.max, random: () => 0.5 };
    const machine = factory(
      FakeWebSocket,
      setTimeoutStub,
      clearTimeoutStub,
      mathStub,
      { protocol: 'http:', host: '127.0.0.1:7509' },
      { warn() {} },
      calls,
    );
    return { machine, calls, advance, timers };
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
    calls.sockets[0].open();
    check('open stops the fallback poll', calls.stops === 1);
    check('open reconciles so nothing is missed across the gap', calls.reconciles >= 1);
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

if (failures > 0) {
  console.error(`permission-ws: ${failures} check(s) failed`);
  process.exit(1);
}
console.log('permission-ws: all checks passed');
