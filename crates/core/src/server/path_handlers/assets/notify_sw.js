// Freenet notification service worker.
//
// Why this exists: mobile browsers (Chrome / Firefox on Android) do NOT support
// the page-level `new Notification(...)` constructor — it throws "Illegal
// constructor. Use ServiceWorkerRegistration.showNotification() instead". The
// ONLY way to show a web notification on mobile is via a service worker's
// showNotification(). Desktop supports both. So the gateway shell
// (shell_bridge.js) registers this worker and calls
// registration.showNotification() so notifications work on mobile as well as
// desktop.
//
// This worker is deliberately minimal:
//   - It has NO `fetch` handler, so it NEVER intercepts, caches, or alters any
//     network request. Registering it changes nothing about how pages, the
//     WASM app, or the node WebSocket load — it exists only to own
//     showNotification() and to route notification clicks back to the shell.
//   - It shows nothing on its own: the shell decides when to call
//     showNotification(), applying the browser-permission + per-contract
//     consent + rate-limit gates in shell_bridge.js. This worker is not a push
//     endpoint (there is no `push` handler); it only displays notifications the
//     shell explicitly asks for while a shell page is open.
//   - Access worker globals through `self.` (self.clients, self.skipWaiting) so
//     the file lints under the browser eslint env without a service-worker env.
'use strict';

self.addEventListener('install', function () {
  // Activate immediately instead of waiting for existing clients to close, so
  // the first notification right after registration can be shown without a
  // page reload.
  self.skipWaiting();
});

self.addEventListener('activate', function (event) {
  // Take control of already-open shell pages so `navigator.serviceWorker.ready`
  // resolves for them and showNotification() works without a reload.
  event.waitUntil(self.clients.claim());
});

// Derive a contract's routing prefix (the FIRST /v[12]/contract/web/<key>/
// segment) from a URL, and select the open window that belongs to it. Anchoring
// on the FIRST segment — the contract its shell actually serves; the shell nav
// proxy pins the leading key — and comparing for EQUALITY is what keeps a click
// for contract A from focusing contract B's tab or leaking A's room tag into it
// (and off the gateway dashboard). A plain substring test would be fooled by a
// crafted same-contract subpath that merely CONTAINS another contract's segment
// (e.g. .../web/BBB/v1/contract/web/AAA/, reachable via the nav proxy). Returns
// null when no open window matches — the caller then opens a fresh window rather
// than handing the tag to an unrelated tab.
//
// notify-pick-client:BEGIN — pure; extracted verbatim between these markers and
// unit-tested by notify_sw.test.mjs. Keep it pure (params/locals only).
function contractPrefixOf(url) {
  if (typeof url !== 'string') return null;
  var m = url.match(/\/v[12]\/contract\/web\/[^/?#]+\//);
  return m ? m[0] : null;
}

function pickNotifyClient(clients, prefix) {
  if (!prefix) return null;
  for (var i = 0; i < clients.length; i++) {
    var c = clients[i];
    if (c && contractPrefixOf(c.url) === prefix) return c;
  }
  return null;
}
// notify-pick-client:END

// A notification was clicked. The click fires HERE (in the worker), not in the
// page, so we focus the originating contract's shell window and hand it the
// click; the shell forwards it to the sandboxed iframe as `notification_click`
// (see shell_bridge.js), which routes the app to the right room. This mirrors
// the page-level `n.onclick` path used on desktop when no service worker is
// active.
self.addEventListener('notificationclick', function (event) {
  event.notification.close();
  var data = event.notification.data || {};
  var tag = typeof data.fnTag === 'string' ? data.fnTag : null;
  var url = typeof data.fnUrl === 'string' ? data.fnUrl : null;
  // The contract-web path prefix (/v[12]/contract/web/<key>/) identifies the
  // contract whose shell showed this notification, so the click routes back to
  // exactly that contract.
  var prefix = contractPrefixOf(url);
  event.waitUntil(
    self.clients
      .matchAll({ type: 'window', includeUncontrolled: true })
      .then(function (clientList) {
        var target = pickNotifyClient(clientList, prefix);
        if (target) {
          if (typeof target.focus === 'function') {
            try {
              target.focus();
            } catch (e) {}
          }
          target.postMessage({ __freenet_notify_click__: true, tag: tag });
          return undefined;
        }
        // No open window on the originating contract: reopen its shell so the
        // click lands on the right app (not the gateway dashboard). Falls back
        // to the origin root only if we somehow have no originating URL.
        if (self.clients.openWindow) {
          return self.clients.openWindow(url || '/');
        }
        return undefined;
      }),
  );
});
