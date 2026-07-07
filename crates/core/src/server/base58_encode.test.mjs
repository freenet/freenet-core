// Executable unit test for the inline `base58Encode` in
// `path_handlers/assets/shell_user_token.js`.
//
// Why this exists: that encoder is a from-scratch big-integer (base-256 ->
// base-58) implementation with edge cases (leading-zero bytes, all-zero input,
// digit-array trimming). It runs in the browser shell, where the surrounding
// IIFE touches browser-only globals (`location`, `localStorage`, `crypto`) and
// so can't run under Node. Rather than pull in a browser test runner, this test
// EXTRACTS the self-contained `base58Encode` function verbatim from the asset
// (between the `base58-encoder:BEGIN`/`:END` markers) and checks its output:
//
//   1. against an INDEPENDENT BigInt reference encoder, exhaustively for all
//      1- and 2-byte inputs and for all-zero / leading-zero shapes — a bug in
//      the extracted encoder is unlikely to be shared by the BigInt reference,
//      which uses a different technique; and
//   2. against a handful of fixed, authoritative bs58 (Bitcoin alphabet)
//      vectors cross-checked against the Rust `bs58` crate — this guards the
//      alphabet itself, which both JS impls would otherwise share.
//
// Run via `npm test` in crates/core/src/server (wired into the lint-assets CI
// job). No dependencies beyond Node's stdlib. Exits non-zero on any mismatch.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'path_handlers/assets/shell_user_token.js');
const src = readFileSync(assetPath, 'utf8');

// --- Extract the encoder verbatim from the asset -------------------------
const BEGIN = 'base58-encoder:BEGIN';
const END = 'base58-encoder:END';
const b = src.indexOf(BEGIN);
const e = src.indexOf(END);
if (b < 0 || e < 0 || e < b) {
  console.error(
    `FAIL: could not find ${BEGIN}/${END} markers in ${assetPath}. ` +
      'The base58 encoder must stay bracketed by those markers so this test ' +
      'can extract and verify it.',
  );
  process.exit(1);
}
const region = src.slice(b, e);
const fnStart = region.indexOf('function base58Encode(');
if (fnStart < 0) {
  console.error('FAIL: no `function base58Encode(` between the markers.');
  process.exit(1);
}
const fnSource = region.slice(fnStart);
// eslint is not run on this file (it lints only assets/**), but keep it clean.
// Build the extracted function in an isolated scope and return it.
const base58Encode = new Function(`${fnSource}\nreturn base58Encode;`)();

// --- Independent reference encoder (BigInt technique) --------------------
const ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function refBase58(bytes) {
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  let n = 0n;
  for (const byte of bytes) n = (n << 8n) | BigInt(byte);
  let s = '';
  while (n > 0n) {
    s = ALPHABET[Number(n % 58n)] + s;
    n /= 58n;
  }
  return ALPHABET[0].repeat(zeros) + s;
}

let failures = 0;
function check(cond, msg) {
  if (!cond) {
    failures++;
    console.error(`FAIL: ${msg}`);
  }
}

function hexToBytes(hex) {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

// --- 0. Alphabet sanity (guards the shared-alphabet risk) ----------------
check(ALPHABET.length === 58, 'alphabet must be 58 chars');
for (const bad of ['0', 'O', 'I', 'l']) {
  check(!ALPHABET.includes(bad), `alphabet must omit ambiguous char '${bad}'`);
}
check(base58Encode(hexToBytes('00')) === '1', 'byte 0x00 -> "1"');
check(base58Encode(hexToBytes('ff')) === '5Q', 'byte 0xff -> "5Q"');

// --- 1. Exhaustive cross-check vs the independent reference ---------------
for (let x = 0; x < 256; x++) {
  const got = base58Encode(Uint8Array.of(x));
  check(got === refBase58(Uint8Array.of(x)), `1-byte [${x}] mismatch (${got})`);
}
for (let x = 0; x < 65536; x++) {
  const arr = Uint8Array.of(x >> 8, x & 0xff);
  const got = base58Encode(arr);
  if (got !== refBase58(arr)) {
    check(false, `2-byte [${x >> 8},${x & 0xff}] mismatch (${got})`);
    break; // one report is enough
  }
}
// All-zero and leading-zero shapes across lengths (up to > token length).
for (let len = 0; len <= 40; len++) {
  const allZero = new Uint8Array(len);
  check(
    base58Encode(allZero) === '1'.repeat(len),
    `all-zero length ${len} must be ${len} '1's`,
  );
  check(base58Encode(allZero) === refBase58(allZero), `all-zero len ${len} vs ref`);
  if (len >= 3) {
    const leading = new Uint8Array(len);
    leading[len - 2] = 0x01;
    leading[len - 1] = 0x2a;
    check(
      base58Encode(leading) === refBase58(leading),
      `leading-zero shape len ${len} vs ref`,
    );
  }
}

// --- 2. Fixed authoritative vectors (verified vs the Rust bs58 crate) -----
// hex input -> expected Bitcoin-alphabet base58.
const vectors = [
  ['', ''],
  ['00', '1'],
  ['0000', '11'],
  ['01', '2'],
  ['0001', '12'],
  ['61', '2g'],
  ['626262', 'a3gV'],
  ['636363', 'aPEr'],
  ['516b6fcd0f', 'ABnLTmg'],
  ['ffeeddccbbaa', '3CSwN61PP'],
  ['73696d706c792061206c6f6e6720737472696e67', '2cFupjhnEsSn59qHXstmK2ffpLv2'],
  [
    '00eb15231dfceb60925886b67d065299925915aeb172c06647',
    '1NS17iag9jJgTHD1VXjvLCEnZuQ3rJDE9L',
  ],
  // 32 bytes of 0xff — the max 32-byte value, matches the token length domain.
  [
    'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
    'JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG',
  ],
];
for (const [hex, expected] of vectors) {
  const got = base58Encode(hexToBytes(hex));
  check(got === expected, `vector ${hex || '(empty)'} -> ${expected}, got ${got}`);
}

// --- 3. Token-shape sanity: a 32-byte input encodes to a base58 string of
//        the length range bs58 produces for 256-bit values (43-44 chars),
//        using only alphabet characters.
const sample = new Uint8Array(32).fill(0x7f);
const enc = base58Encode(sample);
check(
  enc === refBase58(sample),
  `32-byte 0x7f sample must match the independent reference (got ${enc})`,
);
check(
  enc.length >= 43 && enc.length <= 44,
  `32-byte input should encode to 43-44 base58 chars, got ${enc.length}`,
);
check(
  [...enc].every((c) => ALPHABET.includes(c)),
  'encoded token must contain only alphabet characters',
);

if (failures > 0) {
  console.error(`\nbase58Encode: ${failures} check(s) FAILED`);
  process.exit(1);
}
console.log('base58Encode: all checks passed');
