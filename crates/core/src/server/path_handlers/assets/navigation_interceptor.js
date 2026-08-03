(function () {
  'use strict';
  // Shared handler for both `click` (primary button) and `auxclick`
  // (non-primary, i.e. middle-click). Middle-click is dispatched via
  // `auxclick` in modern browsers and does NOT fire `click` at all, so a
  // `click`-only listener would miss it entirely (freenet/freenet-core#3853).
  //
  // New-window activations (target="_blank", ctrl/cmd/middle/shift-click) are
  // deliberately NOT intercepted. The shell iframe carries
  // `allow-popups-to-escape-sandbox`, so a popup it opens is a normal
  // top-level document at the node's real origin: the shell loads, its
  // `frame-src 'self'` matches, `localStorage` (and with it the hosted access
  // key) works, and a cross-origin destination sees a real Origin instead of
  // `null`. That last one also revives the permission surface in the new tab:
  // an opaque-origin tab sends `Origin: null`, and `/permission/pending`
  // answers 200 with an EMPTY list rather than an error, so the prompt simply
  // never appeared and nothing was logged (the cost #5107 priced out on this
  // branch). Routing those through the shell instead put `window.open` inside a
  // `message` handler, which is what broke Firefox — its popup blocker gates
  // on the dispatching event type (`dom.popup_allowed_events`: click,
  // auxclick, … never `message`), while Chrome/Safari allow it by propagating
  // user activation across the frame tree. A real gesture in THIS frame is the
  // only mechanism that opens a tab consistently everywhere.
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
    // An explicit new-window target opens natively (see the note above).
    if (target.target && target.target !== '_self') return;
    // So does a modifier/middle-click new-window activation. The old
    // postMessage route could not preserve these — every one of them collapsed
    // to a plain foreground tab, or to an in-frame navigation for same-origin
    // links. Native handling restores background-tab (ctrl/cmd/middle) and
    // new-window (shift) placement, and the escaped popup lands on the shell
    // at the real origin either way.
    if (e.button === 1 || e.ctrlKey || e.metaKey || e.shiftKey) return;
    // Classify by origin.
    //
    // Fail-safe default: if the origin comparison throws (pathological URLs
    // that slipped past the protocol check above) we assume cross-origin,
    // because the failure mode we are guarding against is replacing the app
    // frame with a document the shell's CSP will refuse, not an accidental
    // in-contract navigation.
    var isCrossOrigin = true;
    try {
      isCrossOrigin = target.origin !== location.origin;
    } catch (err) {}
    if (isCrossOrigin) {
      // Cross-origin link with no new-window target: left alone it would
      // navigate THIS frame, and the shell's `frame-src 'self'` refuses a
      // foreign origin, so the click would silently do nothing. Open a tab
      // instead. `window.open` is called during a `click`/`auxclick` handler —
      // both are in Firefox's `dom.popup_allowed_events` — so the gesture is
      // live and no popup blocker fires. The escaped popup is a real
      // top-level context, so the destination sees a proper Origin
      // (freenet/river#208).
      e.preventDefault();
      window.open(target.href, '_blank', 'noopener,noreferrer');
      return;
    }
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
