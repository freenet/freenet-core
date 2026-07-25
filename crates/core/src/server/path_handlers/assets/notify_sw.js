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

// Choose which open window a notification click routes to: ONLY a window on the
// originating contract's shell path (`prefix`, the /v[12]/contract/web/<key>/
// segment). This keeps a click for contract A from focusing contract B's tab or
// posting A's room tag into B's iframe (a cross-contract leak), and keeps it off
// the gateway dashboard. Returns null when no open window matches — the caller
// then opens a fresh window rather than handing the tag to an unrelated tab.
//
// notify-pick-client:BEGIN — pure; extracted verbatim between these markers and
// unit-tested by notify_sw.test.mjs. Keep it pure (params/locals only).
function pickNotifyClient(clients, prefix) {
  if (!prefix) return null;
  for (var i = 0; i < clients.length; i++) {
    var c = clients[i];
    if (c && typeof c.url === 'string' && c.url.indexOf(prefix) !== -1) {
      return c;
    }
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
  var prefix = null;
  if (url) {
    var m = url.match(/\/v[12]\/contract\/web\/[^/?#]+\//);
    if (m) prefix = m[0];
  }
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
