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
// Slice from the first `function` keyword (the BEGIN marker sits inside a `//`
// comment, so slicing from it would start the region mid-comment-line). The
// region holds both `contractPrefixOf` and `pickNotifyClient`.
const fnStart = src.indexOf('function contractPrefixOf(', b);
if (fnStart < 0 || fnStart > e) {
  console.error('FAIL: no `function contractPrefixOf(` between the markers.');
  process.exit(1);
}
const region = src.slice(fnStart, e);
if (!region.includes('function pickNotifyClient(')) {
  console.error('FAIL: no `function pickNotifyClient(` between the markers.');
  process.exit(1);
}
// eslint-disable-next-line no-new-func
const { contractPrefixOf, pickNotifyClient } = new Function(
  region + '\nreturn { contractPrefixOf: contractPrefixOf, pickNotifyClient: pickNotifyClient };',
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

// contractPrefixOf: derives the FIRST (leftmost) contract segment, so a crafted
// subpath can't move a contract's identity, and non-contract URLs -> null.
check(
  'contractPrefixOf extracts the v1 prefix',
  contractPrefixOf('https://gw.example/v1/contract/web/AAA/x') ===
    '/v1/contract/web/AAA/',
);
check(
  'contractPrefixOf extracts the v2 prefix (ignoring hash)',
  contractPrefixOf('https://gw.example/v2/contract/web/BBB/#room') ===
    '/v2/contract/web/BBB/',
);
check(
  'contractPrefixOf takes the LEFTMOST segment of a crafted subpath',
  contractPrefixOf('https://gw.example/v1/contract/web/BBB/v1/contract/web/AAA/') ===
    '/v1/contract/web/BBB/',
);
check(
  'contractPrefixOf -> null for a non-contract URL',
  contractPrefixOf('https://gw.example/') === null,
);
check(
  'contractPrefixOf -> null for a non-string',
  contractPrefixOf(null) === null,
);

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

// A crafted same-contract subpath that merely CONTAINS another contract's
// segment must NOT be mistaken for it (substring-match bypass; a malicious BBB
// app can reach such a URL via the shell navigation proxy). Anchored equality
// on the FIRST contract segment defeats it.
const bbbSpoof = {
  url: 'https://gw.example/v1/contract/web/BBB/v1/contract/web/AAA/',
};
check(
  'crafted subpath containing another contract segment does NOT match it',
  pickNotifyClient([bbbSpoof], '/v1/contract/web/AAA/') === null,
);
check(
  'the spoof window still matches its OWN contract (BBB)',
  pickNotifyClient([bbbSpoof], '/v1/contract/web/BBB/') === bbbSpoof,
);
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
