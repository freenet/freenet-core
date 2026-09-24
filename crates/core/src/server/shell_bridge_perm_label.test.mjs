// Executable unit test for the permission card's authorship label in
// `path_handlers/assets/shell_bridge.js`.
//
// The label is the only thing on the card that tells delegate-authored text
// from the node's own. The node marks its own capability prompts with
// `author: "node"`; everything else must keep the "Delegate says:" label, or a
// delegate could present its text as Freenet's.
//
// Same extract-verbatim-between-markers technique as
// shell_bridge_permission_ws.test.mjs. Run via `npm test` in
// crates/core/src/server. Exits non-zero on any mismatch.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'path_handlers/assets/shell_bridge.js');
const src = readFileSync(assetPath, 'utf8');

const b = src.indexOf('perm-label:BEGIN');
const e = src.indexOf('perm-label:END');
if (b < 0 || e < 0 || e < b) {
  console.error(`FAIL: perm-label markers not found in ${assetPath}`);
  process.exit(1);
}
const region = src.slice(src.indexOf('function permMessageLabel(', b), e);
const permMessageLabel = new Function(`${region}\nreturn permMessageLabel;`)();

// The card must actually use it, or this test guards nothing.
if (!src.includes('msgLabel.textContent = permMessageLabel(p);')) {
  console.error(
    'FAIL: createCard no longer labels the card with permMessageLabel(p)',
  );
  process.exit(1);
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

check(
  'node-authored prompt says "Freenet asks:"',
  permMessageLabel({ author: 'node' }) === 'Freenet asks:',
);
check(
  'delegate prompt says "Delegate says:"',
  permMessageLabel({ author: 'delegate' }) === 'Delegate says:',
);
check(
  'missing author is delegate-authored',
  permMessageLabel({}) === 'Delegate says:',
);
check(
  'unexpected author is delegate-authored',
  permMessageLabel({ author: 'Node' }) === 'Delegate says:',
);
check(
  'no payload is delegate-authored',
  permMessageLabel(undefined) === 'Delegate says:',
);

if (failures > 0) {
  console.error(`${failures} failure(s)`);
  process.exit(1);
}
console.log('shell_bridge perm label: all checks passed');
