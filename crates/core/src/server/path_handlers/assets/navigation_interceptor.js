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
  // branch).
  //
  // Routing those through the shell instead put `window.open` inside a
  // `message` handler, and that is what broke Firefox (#5106). The mechanism is
  // NOT settled and nothing here depends on which one it is:
  //   - popup blocker — Firefox gates `window.open` on the dispatching event
  //     type (`dom.popup_allowed_events`: click, auxclick, … never `message`),
  //     while Chrome/Safari propagate user activation across the frame tree.
  //     Playwright's patched Firefox does not reproduce this, so it is a
  //     diagnosis from the symptom, not a measurement;
  //   - loopback refusal — #5107 measured that the shell's `open_url` handler
  //     refuses `localhost`/`127.0.0.1`, which drops the forwarded open on a
  //     local node in EVERY engine, and concluded from that matrix that popup
  //     blocking was not involved.
  // Both are consequences of the shell round-trip, and this code removes the
  // round-trip entirely: a real gesture in THIS frame opens a tab everywhere,
  // whichever explanation is right. Do not let either story harden into a fact
  // in a comment without a measurement to cite.
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
    // A target that names a NEW context opens natively (see the note above).
    //
    // Only `_blank` and custom names do. `_top` and `_parent` name an ANCESTOR
    // — an in-place navigation the sandbox forbids without
    // `allow-top-navigation`, so handing one back to the browser is a silently
    // dead click. They must fall through to the classification below and be
    // treated like an untargeted link. Compare lowercased, because browsers
    // match the reserved keywords ASCII-case-insensitively and `target="_SELF"`
    // must mean `_self` here too — otherwise it is read as a new-window request,
    // returns early, and a cross-origin one then replaces the app frame with a
    // document `frame-src 'self'` refuses.
    var targetName = target.target ? String(target.target).toLowerCase() : '';
    if (
      targetName &&
      targetName !== '_self' &&
      targetName !== '_top' &&
      targetName !== '_parent'
    ) {
      return;
    }
    // So does a modifier/middle-click new-window activation. The old
    // postMessage route could not preserve these — every one of them collapsed
    // to a plain foreground tab, or to an in-frame navigation for same-origin
    // links. Native handling restores background-tab (ctrl/cmd/middle) and
    // new-window (shift) placement, and the escaped popup lands on the shell
    // at the real origin either way.
    //
    // `e.button` is truthy for EVERY non-primary button, not just the middle
    // one. `auxclick` fires for the secondary (context-menu) button and for
    // the back/forward buttons as well, and `preventDefault` on it does not
    // suppress the context menu — that comes from `mousedown`. So a check for
    // the middle button alone leaves right-click intercepted: the user gets
    // the menu AND an unwanted tab (cross-origin) or an app-frame navigation
    // (same-origin). Right-clicking a link is also exactly the workaround
    // users adopted while `target="_blank"` was broken.
    if (e.button || e.ctrlKey || e.metaKey || e.shiftKey) return;
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
