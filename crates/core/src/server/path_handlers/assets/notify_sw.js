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

// A notification was clicked. The click fires HERE (in the worker), not in the
// page, so we focus an open shell window and hand it the click; the shell
// forwards it to the sandboxed iframe as `notification_click` (see
// shell_bridge.js), which routes the app to the right room. This mirrors the
// page-level `n.onclick` path used on desktop when no service worker is active.
self.addEventListener('notificationclick', function (event) {
  event.notification.close();
  var data = event.notification.data || {};
  var tag = typeof data.fnTag === 'string' ? data.fnTag : null;
  event.waitUntil(
    self.clients
      .matchAll({ type: 'window', includeUncontrolled: true })
      .then(function (clientList) {
        for (var i = 0; i < clientList.length; i++) {
          var client = clientList[i];
          if (typeof client.focus === 'function') {
            try {
              client.focus();
            } catch (e) {}
          }
          // Hand the click to the shell; it validates and forwards to the
          // iframe. Post to the first window client only — the common case is a
          // single shell tab, and the shell/app ignore a tag for a room they
          // don't hold.
          client.postMessage({ __freenet_notify_click__: true, tag: tag });
          return undefined;
        }
        // No open window (tab was closed): open one at the origin root.
        if (self.clients.openWindow) {
          return self.clients.openWindow('/');
        }
        return undefined;
      }),
  );
});
