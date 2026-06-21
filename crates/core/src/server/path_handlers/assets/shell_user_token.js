var __freenet_user_token = (function() {
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
  if (location.protocol !== 'https:') { return undefined; }
  try {
    var KEY = '__freenet_user_token__';
    var t = localStorage.getItem(KEY);
    if (!t) {
      var b = new Uint8Array(32);
      crypto.getRandomValues(b);
      t = Array.prototype.map.call(b, function(x) {
        return ('0' + x.toString(16)).slice(-2);
      }).join('');
      localStorage.setItem(KEY, t);
    }
    return t;
  } catch (e) {
    return undefined;
  }
})();