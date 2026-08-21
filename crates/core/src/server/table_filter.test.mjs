// Executable test for the long-table filter/collapse decision logic in
// `home_page/assets/dashboard.js`.
//
// Why this exists: the Rust tests in `home_page.rs` assert the emitted MARKUP
// — that the controls are present, labelled, and bound to the right table.
// They never execute a line of `dashboard.js`. Every defect this filter has
// actually had lived in the JS and was invisible to them:
//
//   * the filter matched only `textContent`, so pasting a full contract key
//     — the thing the copy button next to every row puts on your clipboard —
//     matched nothing, because the cell renders a 12-character abbreviation
//     and the full key lives in `title` / `data-copy` / `data-sort`;
//   * sorting a collapsed table left the visibility flags attached to the
//     previously-visible elements, so you saw the right NUMBER of the wrong
//     rows.
//
// Both are decisions, not markup, so they are extracted here (between the
// `tf-search:BEGIN`/`:END` markers) as pure functions and driven under Node.
// The DOM plumbing around them still needs a browser; that is what the
// Playwright suite covers.
//
// Run via `npm test` in crates/core/src/server (wired into the lint-assets CI
// job). No dependencies beyond Node's stdlib. Exits non-zero on any mismatch.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'home_page/assets/dashboard.js');
const src = readFileSync(assetPath, 'utf8');

const BEGIN = 'tf-search:BEGIN';
const END = 'tf-search:END';
const b = src.indexOf(BEGIN);
const e = src.indexOf(END);
if (b < 0 || e < 0 || e < b) {
  console.error(
    `FAIL: could not find ${BEGIN}/${END} markers in ${assetPath}. ` +
      'The filter decision functions must stay bracketed by those markers ' +
      'so this test can extract and verify them.',
  );
  process.exit(1);
}
const region = src.slice(b, e);
for (const name of ['rowSearchText', 'visibleRowFlags']) {
  if (region.indexOf(`function ${name}(`) < 0) {
    console.error(`FAIL: no \`function ${name}(\` between the markers.`);
    process.exit(1);
  }
}
// Trim back to the last closing brace: the region runs up to the END marker,
// so it would otherwise carry that comment's dangling `/*` opener.
const body = region.slice(
  region.indexOf('function rowSearchText('),
  region.lastIndexOf('}') + 1,
);
const { rowSearchText, visibleRowFlags } = new Function(
  `${body}\nreturn { rowSearchText, visibleRowFlags };`,
)();

let failures = 0;
function check(name, pass, detail) {
  if (!pass) failures++;
  console.log(
    `${pass ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`,
  );
}
const eq = (name, got, want) =>
  check(
    name,
    JSON.stringify(got) === JSON.stringify(want),
    JSON.stringify(got) === JSON.stringify(want)
      ? ''
      : `got ${JSON.stringify(got)} want ${JSON.stringify(want)}`,
  );

// --- rowSearchText --------------------------------------------------------

// The regression the attribute-folding exists for. The rendered cell shows an
// abbreviation; the query is the full key.
const FULL_KEY = '7WSdxLxjPvKgGZBqDpRuPMuoprnQBmXtnkHkDpTPTdcJ';
const ABBREV = '7WSdxLxjPvKg…';
check(
  'a full contract key matches a row that only DISPLAYS its abbreviation',
  rowSearchText(`${ABBREV} 4.2 kB 2`, [FULL_KEY]).indexOf(
    FULL_KEY.toLowerCase(),
  ) !== -1,
  'this is the primary way an operator searches for a contract',
);
check(
  'and the abbreviation still matches, so typing a prefix keeps working',
  rowSearchText(`${ABBREV} 4.2 kB 2`, [FULL_KEY]).indexOf('7wsdxlxjpvkg') !==
    -1,
);
eq(
  'null and empty attributes are dropped rather than stringified',
  rowSearchText('Row', [null, '', undefined, 'Kept']),
  'row kept',
);
eq('missing text content is tolerated', rowSearchText(null, []), '');
check(
  'matching is case-insensitive on both sides',
  rowSearchText('MiXeD', ['CaSe']).indexOf('mixed case') !== -1,
);
check(
  'attributes are space-joined, so a match cannot straddle two of them',
  rowSearchText('', ['ab', 'cd']).indexOf('abcd') === -1,
  'guards against a false positive from naive concatenation',
);

// The caller decides WHICH attributes to pass, and the narrowness is the
// point: only `data-copy` may be folded in, because it holds the full form of
// what the cell displays. Pin that here so a future "search more attributes"
// change has to argue with a test rather than slip through.
//
// Widening to `data-sort` or `title` surfaces rows for reasons invisible on
// screen: `data-sort="1048576"` renders as "1.0 MB", and every contract row
// carries a copy button titled "Copy contract key", so `title` would make the
// query "key" match the entire table.
const callerSrc = src.slice(src.indexOf('function applyTableView('));
const callerBody = callerSrc.slice(0, callerSrc.indexOf('\nfunction '));
for (const attr of ['data-sort', 'title']) {
  check(
    `applyTableView does not fold '${attr}' into the searchable text`,
    callerBody.indexOf(`getAttribute('${attr}')`) === -1,
    'it holds a value that differs from what the cell displays',
  );
}
check(
  "applyTableView does fold 'data-copy' in",
  callerBody.indexOf("getAttribute('data-copy')") !== -1,
  'this is what makes a full contract key findable',
);

// --- visibleRowFlags ------------------------------------------------------

const all = (n) => Array.from({ length: n }, () => true);

eq(
  'a short table is shown in full',
  visibleRowFlags(all(3), false, 25),
  [true, true, true],
);
eq(
  'a long table is cut at the cap',
  visibleRowFlags(all(4), false, 2),
  [true, true, false, false],
);
eq(
  '"show all" defeats the cap',
  visibleRowFlags(all(4), true, 2),
  [true, true, true, true],
);

// The load-bearing one: the cap counts MATCHING rows, not row positions. If it
// counted positions, filtering a long table would show a handful of matches
// and hide the rest under a cap the user has already narrowed past.
eq(
  'the cap counts matches, not positions',
  visibleRowFlags([false, false, true, false, true], false, 2),
  [false, false, true, false, true],
);
eq(
  'non-matching rows are hidden regardless of "show all"',
  visibleRowFlags([true, false, true], true, 25),
  [true, false, true],
);
eq('an empty table produces no flags', visibleRowFlags([], false, 25), []);
eq(
  'a zero cap hides everything unless expanded',
  visibleRowFlags(all(2), false, 0),
  [false, false],
);

console.log(failures === 0 ? '\nAll checks passed.' : `\n${failures} failed.`);
process.exit(failures === 0 ? 0 : 1);
