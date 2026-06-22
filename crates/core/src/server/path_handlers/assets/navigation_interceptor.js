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
})();
