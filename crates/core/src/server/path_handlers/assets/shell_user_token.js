var __freenet_user_token = (function () {
  'use strict';
  // REFUSE-PLAINTEXT-TOKEN (client mirror of the backend invariant, #4381):
  // never load OR mint the durable token on a non-secure page. The token is a
  // high-value, node-independent bearer secret; the backend won't HONOR it over
  // plaintext, but only the client can stop it from being TRANSMITTED in the
  // first place. Returning undefined here (BEFORE any localStorage access) means
  // an http page never reads a previously-minted token either, so the stored
  // value is never put on a ws:// URL. The TLS proxy serves the shell over
  // https in the intended hosted deployment; a direct http://localhost dev hit
  // simply gets no token (Local), which is the correct/expected behavior.
  if (location.protocol !== 'https:') {
    return undefined;
  }

  // Minimal, self-contained Base58 encoder (Bitcoin/bs58 alphabet). There is no
  // bs58 in the browser and we deliberately avoid pulling in an external JS
  // dependency, so this inlines the standard big-endian base-256 -> base-58 long
  // division. `bytes` is a Uint8Array (here always the 32 random token bytes);
  // the return value is the base58 string.
  //
  // The server treats the token as an OPAQUE namespace key — it hashes the raw
  // string bytes (blake3(domain || token), see UserSecretContext::from_token) —
  // so the ONLY hard requirement is a stable, valid encoding. Base58 is chosen
  // over hex because it drops the visually ambiguous characters, which makes an
  // "access key" that the user copies, pastes, or transcribes by hand far less
  // error-prone, and it matches the bs58 (Bitcoin alphabet) used by the rest of
  // Freenet's identifiers. This encoder is byte-for-byte compatible with the
  // `bs58` crate's BITCOIN alphabet output.
  function base58Encode(bytes) {
    var ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
    // Leading zero bytes carry no numeric value but must survive as leading
    // '1's (base58's zero digit), so count and skip them before the division.
    var zeros = 0;
    while (zeros < bytes.length && bytes[zeros] === 0) {
      zeros++;
    }
    // Treat the remaining bytes as a big-endian base-256 integer and convert it
    // to base-58, accumulating the base-58 digits little-endian via repeated
    // long division. Intermediate `carry` stays small (< 58 * 256 + 255), well
    // within a 32-bit int, so `<< 8` and `| 0` are safe.
    var digits = [0];
    for (var i = zeros; i < bytes.length; i++) {
      var carry = bytes[i];
      for (var j = 0; j < digits.length; j++) {
        carry += digits[j] << 8;
        digits[j] = carry % 58;
        carry = (carry / 58) | 0;
      }
      while (carry > 0) {
        digits.push(carry % 58);
        carry = (carry / 58) | 0;
      }
    }
    var out = '';
    for (var z = 0; z < zeros; z++) {
      out += ALPHABET[0];
    }
    // Emit base-58 digits most-significant first. Skip a lone most-significant
    // zero digit, which only appears for an all-zero input (already fully
    // represented by the leading '1's above); for any non-zero input the top
    // digit is non-zero.
    var top = digits.length - 1;
    while (top > 0 && digits[top] === 0) {
      top--;
    }
    if (!(top === 0 && digits[0] === 0)) {
      for (var k = top; k >= 0; k--) {
        out += ALPHABET[digits[k]];
      }
    }
    return out;
  }

  try {
    var KEY = '__freenet_user_token__';
    var t = localStorage.getItem(KEY);
    if (!t) {
      var b = new Uint8Array(32);
      crypto.getRandomValues(b);
      // New identities mint a base58 access key. Existing users keep whatever
      // string is already in localStorage verbatim (older builds stored hex),
      // and the "restore" flow in hosted_bar.js stores any pasted key as-is, so
      // both formats keep working: the server hashes the raw token string either
      // way, so a previously stored hex token maps to the same per-user
      // namespace as before and old data stays reachable.
      t = base58Encode(b);
      localStorage.setItem(KEY, t);
    }
    return t;
  } catch (e) {
    return undefined;
  }
})();
