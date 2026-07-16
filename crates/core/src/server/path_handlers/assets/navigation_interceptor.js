(function () {
  'use strict';
  // Shared handler for both `click` (primary button) and `auxclick`
  // (non-primary, i.e. middle-click). Middle-click is dispatched via
  // `auxclick` in modern browsers and does NOT fire `click` at all, so
  // without a separate `auxclick` listener middle-clicks on cross-origin
  // <a target="_blank"> links bypass the interceptor entirely and the
  // browser opens a null-origin sandboxed popup (freenet/freenet-core#3853
  // follow-up from #3852).
  function handleAnchorClick(e) {
    var target = e.target;
    // Walk up to find the nearest <a> element (handles clicks on child elements)
    while (target && target.tagName !== 'A') target = target.parentElement;
    if (!target || !target.href) return;
    // Skip javascript: and mailto: links
    var protocol = target.protocol;
    if (protocol && protocol !== 'http:' && protocol !== 'https:') return;
    // Skip links with download attribute
    if (target.hasAttribute('download')) return;
    // Skip links explicitly marked to bypass interception
    if (target.dataset && target.dataset.freenetNoIntercept) return;
    // Classify by origin. Cross-origin always goes through the open_url
    // bridge, regardless of the `target` attribute, because a sandboxed
    // popup would have a null origin and break CORS on the destination
    // (freenet/river#208).
    //
    // Fail-safe default: if the origin comparison throws (pathological URLs
    // that slipped past the protocol check above) we assume cross-origin,
    // because the failure mode we are guarding against is a null-origin
    // sandboxed popup, not an accidental in-contract navigation.
    var isCrossOrigin = true;
    try {
      isCrossOrigin = target.origin !== location.origin;
    } catch (err) {}
    if (isCrossOrigin) {
      e.preventDefault();
      // Forward shift-key state so the shell can honour shift-click
      // as a new-window request (freenet/freenet-core#3853). ctrl /
      // meta / middle-click intent can't be meaningfully preserved
      // from a postMessage handler: browsers only allow background-
      // tab placement when window.open is called directly from a
      // user gesture, and all three collapse to a plain new tab
      // regardless of what we forward. Keep the contract minimal.
      window.parent.postMessage(
        {
          __freenet_shell__: true,
          type: 'open_url',
          url: target.href,
          shiftKey: !!e.shiftKey,
        },
        '*',
      );
      return;
    }
    // Same-origin link. Respect explicit non-_self targets so webapps
    // that open multiple tabs within their own contract still work.
    if (target.target && target.target !== '_self') return;
    // Same-origin in-contract link: request navigation via shell
    e.preventDefault();
    window.parent.postMessage(
      {
        __freenet_shell__: true,
        type: 'navigate',
        href: target.href,
      },
      '*',
    );
  }
  document.addEventListener('click', handleAnchorClick, true);
  // Catch middle-click and other non-primary button activations.
  document.addEventListener('auxclick', handleAnchorClick, true);

  // Route programmatic `window.open()` through the shell too, mirroring the
  // anchor interceptor above. A sandboxed iframe has an opaque origin and no
  // `allow-popups-to-escape-sandbox`, so a popup it opens DIRECTLY inherits the
  // sandbox: null origin, no localStorage. On a hosted node that dead-ends on
  // the "Open this app in a normal tab" per-user-isolation page, because the
  // opaque-origin tab can't read the per-user access key (freenet-core#4645).
  // The click/auxclick listeners already cover <a> activations, but an app that
  // calls window.open() from its own JS bypasses them. The shell runs in the
  // real origin, so having IT open the URL (via the same `open_url` bridge the
  // cross-origin anchor path uses) yields a normal tab that works.
  //
  // The returned WindowProxy is intentionally dropped (return null). A popup
  // opened from here would be a DIFFERENT opaque origin than this document, so
  // the opener could never script it across that boundary anyway; nothing is
  // lost by not handing back a reference, and null matches the shell's
  // `noopener` open. Non-http(s) targets (empty/about:blank, javascript:,
  // blob:, data:) fall back to the native open so unrelated behavior is
  // unchanged; `open_url`'s scheme allow-list is the security gate for the
  // forwarded case, exactly as for anchor clicks.
  var nativeWindowOpen =
    typeof window.open === 'function' ? window.open.bind(window) : null;
  function fallbackOpen(url, name, features) {
    return nativeWindowOpen ? nativeWindowOpen(url, name, features) : null;
  }
  window.open = function (url, name, features) {
    // Only intercept a genuinely nested context that has a shell to forward to.
    // A top-level document (no distinct parent) keeps native behavior.
    if (!window.parent || window.parent === window) {
      return fallbackOpen(url, name, features);
    }
    if (typeof url !== 'string' || url === '') {
      return fallbackOpen(url, name, features);
    }
    var resolved;
    try {
      // Resolve relative targets (e.g. window.open('page2')) against the
      // iframe's base so the shell receives an absolute URL; new URL() with a
      // relative string and no base would throw.
      resolved = new URL(url, document.baseURI);
    } catch (err) {
      return fallbackOpen(url, name, features);
    }
    if (resolved.protocol !== 'http:' && resolved.protocol !== 'https:') {
      return fallbackOpen(url, name, features);
    }
    window.parent.postMessage(
      {
        __freenet_shell__: true,
        type: 'open_url',
        url: resolved.href,
        // window.open is an explicit new-window request; the shell opens a
        // fresh tab (shiftKey:false matches the plain-left-click default).
        shiftKey: false,
      },
      '*',
    );
    return null;
  };
})();
