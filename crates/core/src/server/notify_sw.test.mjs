// Node unit test for `notify_sw.js`'s `pickNotifyClient` client-selection
// helper — extracted verbatim between its `notify-pick-client:BEGIN`/`:END`
// markers (same technique as shell_bridge_notifications.test.mjs). This is the
// notification-click routing that keeps a click for one contract from focusing
// or leaking its room tag into another contract's tab.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'path_handlers/assets/notify_sw.js');
const src = readFileSync(assetPath, 'utf8');

const BEGIN = 'notify-pick-client:BEGIN';
const END = 'notify-pick-client:END';
const b = src.indexOf(BEGIN);
const e = src.indexOf(END);
if (b < 0 || e < 0) {
  console.error(
    `FAIL: could not find ${BEGIN}/${END} markers in ${assetPath}. The ` +
      'pickNotifyClient helper must stay bracketed by those markers so this ' +
      'test can extract and verify it.',
  );
  process.exit(1);
}
// Slice from the `function` keyword (the BEGIN marker sits inside a `//`
// comment, so slicing from it would start the region mid-comment-line).
const fnStart = src.indexOf('function pickNotifyClient(', b);
if (fnStart < 0 || fnStart > e) {
  console.error('FAIL: no `function pickNotifyClient(` between the markers.');
  process.exit(1);
}
const region = src.slice(fnStart, e);
// eslint-disable-next-line no-new-func
const pickNotifyClient = new Function(
  region + '\nreturn pickNotifyClient;',
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

const a = { url: 'https://gw.example/v1/contract/web/AAA/' };
const b2 = { url: 'https://gw.example/v2/contract/web/BBB/#room' };
const dash = { url: 'https://gw.example/' };

// Picks the window on the originating contract's path — even when other
// contract windows and the dashboard are open, and regardless of order.
check(
  'picks the matching v1 contract window',
  pickNotifyClient([dash, b2, a], '/v1/contract/web/AAA/') === a,
);
check(
  'picks the matching v2 contract window (with a hash on the url)',
  pickNotifyClient([a, b2], '/v2/contract/web/BBB/') === b2,
);

// Never routes to a DIFFERENT contract or the dashboard: returns null so the
// caller opens a fresh window instead of leaking the tag to an unrelated tab.
check(
  'no matching window -> null (must not leak to another contract)',
  pickNotifyClient([dash, b2], '/v1/contract/web/AAA/') === null,
);
check(
  'dashboard-only -> null (a contract click never lands on the gateway home)',
  pickNotifyClient([dash], '/v1/contract/web/AAA/') === null,
);
check(
  'empty client list -> null',
  pickNotifyClient([], '/v1/contract/web/AAA/') === null,
);
check('null prefix -> null', pickNotifyClient([a, b2], null) === null);
// Defensive: a client with no url string must not throw or match.
check(
  'client without a url string is skipped',
  pickNotifyClient([{}, a], '/v1/contract/web/AAA/') === a,
);

if (failures) {
  console.error(`notify-sw: ${failures} check(s) failed`);
  process.exit(1);
}
console.log('notify-sw: all checks passed');
