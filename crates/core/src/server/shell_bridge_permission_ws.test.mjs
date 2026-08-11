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

if (failures > 0) {
  console.error(`permission-ws: ${failures} check(s) failed`);
  process.exit(1);
}
console.log('permission-ws: all checks passed');
