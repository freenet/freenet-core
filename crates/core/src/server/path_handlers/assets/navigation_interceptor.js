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
  // Behavior notes (this changes window.open semantics for contract apps, so be
  // precise about what is and isn't forwarded):
  //   - Only NEW-window requests are forwarded. `_self`/`_parent`/`_top` name
  //     an EXISTING context (in-place navigation, no sandbox-inheriting popup),
  //     so they stay native.
  //   - The `name` (window name) and `features` (size/position) arguments are
  //     dropped for a forwarded open: the shell always opens a fresh tab. Named
  //     -window reuse and popup sizing don't survive; on a hosted node such a
  //     popup was a dead sandboxed context anyway, so this is not a regression
  //     there.
  //   - The returned WindowProxy is dropped (return null). A popup opened from
  //     here is a DIFFERENT opaque origin the opener could never script across
  //     anyway, and null matches the shell's `noopener` open. Chained callers
  //     (`window.open(url).focus()`) will throw on null — an accepted break, as
  //     that popup was a dead end on a hosted node.
  //   - Non-http(s) targets (empty/about:blank, javascript:, blob:, data:) and
  //     URL objects/other args are handled below; `open_url`'s scheme allow-list
  //     is the security gate for whatever IS forwarded, exactly as for anchors.
  //   - Loopback targets (localhost/127.0.0.1/...) are NOT forwarded: the shell
  //     `open_url` handler refuses them, so forwarding would silently drop the
  //     open. We fall back to native there, preserving prior local-node behavior
  //     (local nodes have no per-user dead-end to fix). Hosted nodes use a real
  //     domain, so this never affects the case #4645 is about.
  var nativeWindowOpen =
    typeof window.open === 'function' ? window.open.bind(window) : null;
  function fallbackOpen(url, name, features) {
    return nativeWindowOpen ? nativeWindowOpen(url, name, features) : null;
  }
  // Kept in sync with the loopback block in shell_bridge.js's open_url handler:
  // forward only what that handler will actually open. WHATWG `URL.hostname`
  // serializes an IPv6 literal WITH brackets (`[::1]`), so strip them before
  // comparing or `http://[::1]/` would slip past the loopback fallback.
  function isLoopbackHost(hostname) {
    var h = hostname.toLowerCase().replace(/^\[/, '').replace(/\]$/, '');
    return (
      h === 'localhost' || h === '127.0.0.1' || h === '::1' || h === '0.0.0.0'
    );
  }
  window.open = function (url, name, features) {
    // Only intercept when this frame is the shell's DIRECT child. A top-level
    // document (parent === window) keeps native behavior; so does a DEEPER
    // descendant (a contract that embeds another served page), whose parent is
    // an app frame rather than the shell — forwarding open_url there would post
    // to the app frame, which the shell never sees, silently losing the open.
    // For the direct app frame, parent and top are both the shell.
    if (
      !window.parent ||
      window.parent === window ||
      window.parent !== window.top
    ) {
      return fallbackOpen(url, name, features);
    }
    // In-place navigation targets are not new-window requests; leave to native.
    // The reserved keywords are ASCII case-insensitive natively, so normalize
    // (coerce + lowercase) before comparing; window.open(url, '_SELF') must
    // still navigate in place, not open a tab. `_blank`/custom names are new
    // windows and stay intercepted.
    var targetName = name == null ? '' : String(name).toLowerCase();
    if (
      targetName === '_self' ||
      targetName === '_parent' ||
      targetName === '_top'
    ) {
      return fallbackOpen(url, name, features);
    }
    // An OMITTED target (window.open()) is native about:blank; keep it native so
    // `var w = window.open(); w.document.write(...)` flows are unchanged. Explicit
    // null is NOT omitted: native Web IDL coerces it to the string "null" (a
    // relative URL), so let it fall through to the string coercion below.
    if (url === undefined) {
      return fallbackOpen(url, name, features);
    }
    // Coerce URL objects / other stringifiables the way the native API does,
    // so window.open(new URL(...)) is forwarded rather than sent to native
    // (which would recreate the sandbox-inherited dead end this patch prevents).
    var urlStr;
    try {
      urlStr = String(url);
    } catch (err) {
      return fallbackOpen(url, name, features);
    }
    if (urlStr === '') {
      return fallbackOpen(url, name, features);
    }
    // Resolve relative targets (e.g. window.open('page2')) against the iframe's
    // base so the shell receives an absolute URL.
    var resolved;
    try {
      resolved = new URL(urlStr, document.baseURI);
    } catch (err) {
      return fallbackOpen(url, name, features);
    }
    if (resolved.protocol !== 'http:' && resolved.protocol !== 'https:') {
      return fallbackOpen(url, name, features);
    }
    if (isLoopbackHost(resolved.hostname)) {
      return fallbackOpen(url, name, features);
    }
    // Strip the internal `__sandbox` routing param, but ONLY from a SAME-ORIGIN
    // (this node's own) target, and ONLY the raw pair via string surgery.
    //   - Why strip: `__sandbox=1` reaches the resolved URL both when a hash-only
    //     / query-relative target (window.open('#x')) inherits it from the base
    //     AND when an app duplicates its page (window.open(location.href), an
    //     absolute same-origin URL). Opened top-level, the shell redirects
    //     `?__sandbox=1` to the shell root, DROPPING the subpath and app params
    //     (e.g. an invitation). Removing it opens the page the app intended.
    //   - Why same-origin only: an ABSOLUTE EXTERNAL target is forwarded
    //     byte-for-byte — its `__sandbox` (if any) is the destination's, not
    //     ours, and reserializing its query could break signed/opaque links.
    //   - Why string surgery, not URLSearchParams.delete(): delete() reserializes
    //     the whole query, form-encoding co-inherited params (`%20`->`+`,
    //     `~`->`%7E`); a raw-pair filter preserves the kept params' bytes.
    var gatewayOrigin;
    try {
      gatewayOrigin = new URL(document.baseURI).origin;
    } catch (err) {
      gatewayOrigin = null;
    }
    if (gatewayOrigin && resolved.origin === gatewayOrigin && resolved.search) {
      var kept = resolved.search
        .slice(1)
        .split('&')
        .filter(function (pair) {
          return pair !== '__sandbox' && pair.slice(0, 10) !== '__sandbox=';
        });
      resolved.search = kept.join('&');
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
