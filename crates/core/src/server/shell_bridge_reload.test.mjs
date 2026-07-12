// Executable unit test for the shell's peer-restart recovery safeguards in
// `path_handlers/assets/shell_bridge.js` (PR #4781, server-4401 design):
//
//   - `isTrustedStaleTokenClose` / `clampProxiedCloseCode` — the recovery is
//     driven by the node's TRUSTED 4401 close code, and a sandboxed contract
//     cannot forge it by asking the shell to close its own socket.
//   - `reloadUrlCapDecision` — a fail-CLOSED, storage-independent cap on
//     recovery reloads: the count lives in the `_freload` URL param (not
//     writable by the contract, always present even with no sessionStorage), so
//     a reload loop is bounded even in private/no-storage mode.
//
// Same extract-verbatim-between-markers technique as
// shell_bridge_notifications.test.mjs. Run via `npm test` in
// crates/core/src/server (wired into the lint-assets CI job). Exits non-zero on
// any mismatch.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'path_handlers/assets/shell_bridge.js');
const src = readFileSync(assetPath, 'utf8');

// Return the source from the first occurrence of `needle` (a `function` decl)
// up to the END marker — dropping the marker-comment prose that precedes it, so
// `new Function` parses only real code.
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

// --- Extract the pure functions verbatim ---------------------------------
const reloadUrlCapDecision = new Function(
  `${extractFrom('reload-url-cap:BEGIN', 'reload-url-cap:END', 'function reloadUrlCapDecision(')}\nreturn reloadUrlCapDecision;`,
)();

const { isTrustedStaleTokenClose, clampProxiedCloseCode } = new Function(
  `${extractFrom('close-recovery-decision:BEGIN', 'close-recovery-decision:END', 'function isTrustedStaleTokenClose(')}\nreturn { isTrustedStaleTokenClose, clampProxiedCloseCode };`,
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

// 1. (a) A SERVER-initiated 4401 close triggers recovery; (b) an
//    IFRAME-requested (clientClosed) 4401 close is IGNORED.
{
  check(
    'server-initiated 4401 close triggers recovery',
    isTrustedStaleTokenClose(4401, false) === true,
  );
  check(
    'iframe-initiated 4401 close is IGNORED (cannot forge the trigger)',
    isTrustedStaleTokenClose(4401, true) === false,
  );
  check(
    'a normal (1000) close does not trigger recovery',
    isTrustedStaleTokenClose(1000, false) === false,
  );
  check(
    'an abnormal (1006) close does not trigger recovery',
    isTrustedStaleTokenClose(1006, false) === false,
  );
}

// 2. The close proxy clamps app-range codes so an iframe can't even surface
//    4401 to onclose (second, independent guard).
{
  check('clamps 4401 -> 1000', clampProxiedCloseCode(4401) === 1000);
  check('clamps 4000 -> 1000', clampProxiedCloseCode(4000) === 1000);
  check('clamps 4999 -> 1000', clampProxiedCloseCode(4999) === 1000);
  check('leaves 1000 alone', clampProxiedCloseCode(1000) === 1000);
  check('leaves 3000 (non app-range) alone', clampProxiedCloseCode(3000) === 3000);
  check('leaves undefined alone', clampProxiedCloseCode(undefined) === undefined);
}

// 3. reloadUrlCapDecision: fail-CLOSED URL cap. No sessionStorage is touched —
//    the cap lives entirely in the `_freload` URL param — so it holds in
//    private/no-storage mode (the fail-open bug the old design had). Simulate a
//    reload loop by feeding each decision's URL back in at a fixed `now`.
{
  const NOW = 1_000_000;
  let href = 'https://node.example/v1/contract/web/KEY/';
  let allowed = 0;
  let refusedAt = -1;
  for (let i = 0; i < 6; i++) {
    const d = reloadUrlCapDecision(href, NOW);
    if (d.allow) {
      allowed++;
      href = d.url; // carry the incremented count forward, as location.replace would
    } else {
      refusedAt = i;
      break;
    }
  }
  check('cap allows exactly MAX=3 reloads in the window', allowed === 3);
  check('cap fails CLOSED on the 4th reload (loop bounded, no storage)', refusedAt === 3);
  check('the count is carried in the _freload URL param', /_freload=\d+-3/.test(href));
}

// 4. Rolling window: once the window elapses, a fresh budget is granted (so a
//    genuine later restart can still recover) — but only then.
{
  const start = 2_000_000;
  // A URL whose window is already maxed (count 3) started at `start`.
  const maxed = 'https://node.example/app/?_freload=' + start + '-3';
  const withinWindow = reloadUrlCapDecision(maxed, start + 59999);
  check('a maxed window within 60s is refused', withinWindow.allow === false);
  const afterWindow = reloadUrlCapDecision(maxed, start + 60000);
  check(
    'a reload whose window elapsed (>=60s) gets a fresh budget',
    afterWindow.allow === true,
  );
  check(
    'the fresh window restarts the count at 1',
    /_freload=\d+-1/.test(afterWindow.url),
  );
}

// 5. A malformed `_freload` (only reachable by a user hand-editing the URL,
//    never by the sandboxed contract) resets to a fresh window rather than
//    wedging — the loop bound still holds because each reload re-increments.
{
  const d = reloadUrlCapDecision(
    'https://node.example/app/?_freload=garbage',
    3_000_000,
  );
  check('malformed _freload resets to a fresh, allowed window', d.allow === true);
  check(
    'malformed _freload is replaced with a well-formed count',
    /_freload=\d+-1/.test(d.url),
  );
}

// 6. A FUTURE-dated window start (backward wall-clock correction) is not
//    trusted — it resets to a fresh window rather than wedging the cap.
{
  const d = reloadUrlCapDecision(
    'https://node.example/app/?_freload=99999999999999-3',
    3_500_000,
  );
  check('future-dated window start resets to a fresh, allowed window', d.allow === true);
}

if (failures > 0) {
  console.error(`\nshell-bridge-reload: ${failures} check(s) FAILED`);
  process.exit(1);
}
console.log('shell-bridge-reload: all checks passed');
