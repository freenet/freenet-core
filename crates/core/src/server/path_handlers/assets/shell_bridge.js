function freenetBridge(authToken, userToken, hostedMode) {
  'use strict';
  var LOCAL_API_ORIGIN = location.origin;
  var MAX_CONNECTIONS = 32;
  var iframe = document.getElementById('app');
  var connections = new Map();
  var lastClipboard = 0;
  var lastDownload = 0;
  var notifyAffordanceShown = false;
  var notifySnoozedThisSession = false;
  // Fallback consent store for when localStorage is unavailable (private mode),
  // so consent still works for the current session and 'granted' is honest.
  var inMemoryConsent = Object.create(null);
  // Per-tag + global rate limiter for the notification proxy (see the
  // marker-bracketed `makeNotifyRateLimiter` factory below, unit-tested in
  // shell_bridge_notifications.test.mjs).
  var notifyLimiter = makeNotifyRateLimiter();

  // FAIL CLOSED whenever a HOSTED browser has no usable per-user token (#4381).
  //
  // In hosted mode a browser must ALWAYS operate under its own per-user token;
  // there is no legitimate "hosted browser as anonymous Local" state — that is
  // exactly the shared-namespace contamination we refuse. The backend's
  // permissive no-token -> Local mapping exists for the gate's own first-connect
  // reasons, but the shell enforces per-user for browsers by refusing when it
  // has no token.
  //
  // `userToken` ends up undefined for either reason, and BOTH must fail closed:
  //   - hosted + http: the plaintext-transmit guards (SHELL_USER_TOKEN_JS's
  //     `location.protocol !== 'https:'` early return + the attach-side https
  //     guard) deliberately withhold the token, so it arrives undefined here.
  //   - hosted + https + storage/crypto failure: localStorage unavailable, or
  //     crypto.getRandomValues / setItem throws, so SHELL_USER_TOKEN_JS's catch
  //     returns undefined.
  // Keying off `!userToken` (rather than re-checking the protocol) covers both
  // with one condition. hosted + https + token-minted -> userToken truthy ->
  // operate (per-user), unchanged. Non-hosted -> hostedMode undefined -> false
  // -> inert, unchanged.
  //
  // We do NOT load the app iframe (so it cannot operate on the shared Local
  // namespace) and we render a clear message instead; the WS-open handler
  // below also refuses every connection while in this state, as a second
  // independent barrier.
  var hostedNoToken = hostedMode === true && !userToken;
  if (hostedNoToken) {
    // The token is missing for one of three reasons; tailor the guidance so
    // the user can actually recover instead of hitting a dead end (#4645).
    // `window.origin` serializes to the string "null" for an opaque
    // (sandboxed) origin, which is the tell-tale of the DOMINANT case: this
    // page was opened as a NEW TAB/WINDOW from inside a Freenet app (the
    // browser's "open link in new tab", a middle-click, a right-click menu,
    // window.open, or a target=_blank link). Such a context inherits the app
    // iframe's sandbox, so it has an opaque origin, so localStorage throws and
    // the per-user token can't be read. Re-opening the SAME address as a
    // normal top-level tab gets a real origin and works. The other two cases
    // (served over plain http, or storage genuinely disabled) can't be fixed
    // by re-opening, so they get their own guidance.
    var opaqueOrigin = window.origin === 'null';
    var plaintext = location.protocol !== 'https:';
    // Plaintext is the HARD blocker: over http the token is never minted or
    // transmitted (SHELL_USER_TOKEN_JS refuses), so re-opening in a normal tab
    // still fails until the connection is https. So when the page is served
    // insecurely that guidance wins even if the tab is ALSO a sandboxed popup.
    // Only a SECURE sandboxed tab is recoverable by re-opening, so that is the
    // one case that gets the "open in a normal tab" copy-URL affordance.
    var reopenCase = opaqueOrigin && !plaintext;
    var panel = document.createElement('div');
    panel.setAttribute('role', 'alert');
    panel.style.cssText =
      'position:fixed;inset:0;display:flex;align-items:center;' +
      'justify-content:center;padding:2rem;box-sizing:border-box;' +
      'font:16px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;' +
      'color:#1a1a1a;background:#fff;';
    var inner = document.createElement('div');
    inner.style.cssText = 'max-width:34rem;width:100%;text-align:left;';
    var h = document.createElement('h1');
    h.style.cssText = 'font-size:1.25rem;margin:0 0 0.75rem;';
    var lead = document.createElement('p');
    lead.style.cssText = 'margin:0 0 1rem;';
    if (plaintext) {
      h.textContent = 'Secure connection required';
      lead.textContent =
        'This hosted Freenet node needs an https:// connection to protect ' +
        'your per-user access key, so the app won’t load over plain ' +
        'http. Reconnect using the https:// address.';
    } else if (opaqueOrigin) {
      h.textContent = 'Open this app in a normal tab';
      lead.textContent =
        'This page opened in a new tab or window from inside a Freenet app ' +
        '(for example via "open link in new tab", a middle-click, or a ' +
        'right-click menu). Tabs opened that way run in a restricted mode ' +
        'that can’t reach the access key this hosted node uses to keep ' +
        'your data separate, so the app won’t load here.';
    } else {
      h.textContent = 'Browser storage required';
      lead.textContent =
        'This hosted Freenet node keeps a per-user access key in your ' +
        'browser to keep your data separate, but storage is unavailable ' +
        'here (it can be blocked in private-browsing mode or by browser ' +
        'settings). Enable storage for this site, or use a different ' +
        'browser, then reload.';
    }
    inner.appendChild(h);
    inner.appendChild(lead);
    // For the secure restricted-tab case, re-opening the same address in a
    // normal tab is the fix, so surface the URL with a one-click copy. We
    // deliberately do NOT offer a "retry" button that re-opens the app in a
    // popup: a popup spawned from this (already sandboxed) context would
    // inherit the sandbox again and hit the exact same dead end, so the user
    // must open a fresh top-level tab themselves. (Gated on reopenCase, not
    // opaqueOrigin, so an http+sandboxed page doesn't offer to re-open a URL
    // that still can't mint a token — see the plaintext heading above.)
    if (reopenCase) {
      var howto = document.createElement('p');
      howto.style.cssText = 'margin:0 0 0.5rem;';
      howto.textContent =
        'To continue, open this address in a normal browser tab ' +
        '(one you start yourself, e.g. Ctrl/Cmd+T, then paste):';
      inner.appendChild(howto);
      var row = document.createElement('div');
      row.style.cssText = 'display:flex;gap:0.5rem;flex-wrap:wrap;';
      var field = document.createElement('input');
      field.type = 'text';
      field.readOnly = true;
      field.value = location.href;
      field.style.cssText =
        'flex:1 1 16rem;min-width:0;padding:0.5rem 0.6rem;' +
        'font:13px/1.4 ui-monospace,SFMono-Regular,Menlo,monospace;' +
        'border:1px solid #ccc;border-radius:6px;color:#1a1a1a;background:#fafafa;';
      field.addEventListener('focus', function () {
        field.select();
      });
      field.addEventListener('click', function () {
        field.select();
      });
      var copyBtn = document.createElement('button');
      copyBtn.type = 'button';
      copyBtn.textContent = 'Copy address';
      copyBtn.style.cssText =
        'padding:0.5rem 0.9rem;border:1px solid #2563eb;border-radius:6px;' +
        'background:#2563eb;color:#fff;font:14px system-ui,sans-serif;' +
        'cursor:pointer;';
      var status = document.createElement('span');
      status.setAttribute('role', 'status');
      status.style.cssText = 'align-self:center;font-size:13px;color:#4b5563;';
      copyBtn.addEventListener('click', function () {
        function fallback() {
          field.select();
          status.textContent = 'Press Ctrl/Cmd+C to copy';
        }
        // navigator.clipboard is often unavailable or rejects in a sandboxed
        // (opaque-origin) context, so always keep the select-and-Ctrl+C path.
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(location.href).then(
            function () {
              status.textContent = 'Copied';
            },
            function () {
              fallback();
            },
          );
        } else {
          fallback();
        }
      });
      row.appendChild(field);
      row.appendChild(copyBtn);
      inner.appendChild(row);
      inner.appendChild(status);
    }
    panel.appendChild(inner);
    // Remove the (not-yet-loaded, data-src) iframe and show the message. The
    // iframe never had its .src set, so the app never started.
    if (iframe && iframe.parentNode) {
      iframe.parentNode.removeChild(iframe);
    }
    document.body.appendChild(panel);
    document.title = 'Freenet: app not loaded here';
    // Keep listening for iframe messages purely so the WS-open handler can
    // return an 'error' to any app that somehow loaded; but we never set
    // iframe.src, so in practice nothing runs. Fall through to install the
    // message listener (whose 'open' case refuses while hostedNoToken).
  } else {
    // Build iframe src from data-src, appending any URL hash for deep
    // linking. Using data-src (not src) in the HTML means the iframe
    // doesn't start loading until we set .src here, so there is exactly
    // one load -- with the hash already in the URL.
    var iframeDataSrc = iframe.getAttribute('data-src');
    // Cache the contract web prefix; used by nav/popstate path validation.
    // Cross-contract navigation updates this when it accepts a new path
    // so it always reflects the currently loaded contract (see navigate
    // handler below).
    var CONTRACT_PREFIX_RE = /^(\/v[12]\/contract\/web\/[^/]+\/)/;
    var contractPrefixMatch = iframeDataSrc.match(CONTRACT_PREFIX_RE);
    var contractPrefix = contractPrefixMatch ? contractPrefixMatch[1] : null;
    var iframeSrc = iframeDataSrc;
    if (location.hash) {
      iframeSrc += location.hash.slice(0, 8192);
    }
    iframe.src = iframeSrc;
    // Seed history state so that back-navigating to the initial entry still
    // has an identifiable __freenet_nav__ record. Using replaceState avoids
    // adding a new entry — we just tag the existing one.
    if (contractPrefix) {
      try {
        history.replaceState(
          { __freenet_nav__: true, iframePath: iframeSrc },
          '',
        );
      } catch (e) {}
    }
  } // end of the non-fail-closed iframe-load block

  function sendToIframe(msg) {
    if (!iframe || !iframe.contentWindow) return;
    iframe.contentWindow.postMessage(msg, '*');
  }

  // --- Browser notifications proxy ---------------------------------------
  // The app runs in the sandboxed (opaque-origin) iframe and CANNOT use the
  // Notifications API. The shell is same-origin with the node (a real origin)
  // and can. So the app postMessages the shell, which shows the notification.
  //
  // Permission scope: the Notifications permission is per-ORIGIN, and every
  // contract on this gateway shares the shell's origin (they differ only by
  // path). A browser grant is therefore gateway-WIDE. To keep it effectively
  // per-contract we additionally require a stored per-contract consent flag
  // before EVER showing a notification, and we only ask for the browser
  // permission from a real click on the in-shell affordance below. So one
  // contract's grant never lets a different contract on the same gateway
  // notify the user without its own explicit opt-in.
  function contractConsentKey() {
    // Derive the contract key from the trusted, server-routed path
    // (/v[12]/contract/web/<KEY>/...), NEVER from message content — otherwise a
    // contract could claim another contract's consent. Covers both API versions
    // (v1 and v2) so a v2 load isn't stranded. This is a looser, unanchored
    // match than CONTRACT_PREFIX_RE (which is anchored), which is fine because
    // `location.pathname` is server-controlled and can't contain `?`/`#`.
    var m = location.pathname.match(/\/v[12]\/contract\/web\/([^/?#]+)/);
    return m ? 'freenet_notify:' + m[1] : null;
  }

  function contractSnoozeKey() {
    var k = contractConsentKey();
    return k ? k + ':snooze' : null;
  }

  function contractHasConsent() {
    var k = contractConsentKey();
    if (!k) return false;
    try {
      if (localStorage.getItem(k) === 'granted') return true;
    } catch (e) {}
    // Fall back to the in-memory record so a private-mode shell (where
    // localStorage throws) still delivers this session's notifications.
    return inMemoryConsent[k] === true;
  }

  // Records consent; returns false only when there's no contract key to gate on
  // (so the caller must not report 'granted'). Always records in memory so the
  // session works even if persistence fails; persistence is best-effort.
  function setContractConsent() {
    var k = contractConsentKey();
    if (!k) return false;
    inMemoryConsent[k] = true;
    try {
      localStorage.setItem(k, 'granted');
    } catch (e) {}
    return true;
  }

  var NOTIFY_SNOOZE_MS = 24 * 60 * 60 * 1000; // 24h "Not now" cooldown

  function isNotifySnoozed() {
    if (notifySnoozedThisSession) return true;
    var k = contractSnoozeKey();
    if (!k) return false;
    try {
      var v = localStorage.getItem(k);
      if (!v) return false;
      var ts = parseInt(v, 10);
      return isFinite(ts) && Date.now() - ts < NOTIFY_SNOOZE_MS;
    } catch (e) {
      return false;
    }
  }

  function setNotifySnoozed() {
    // In-memory flag makes dismissal effective THIS session even against a
    // contract that spams notification_enable_prompt; the persisted timestamp
    // makes "Not now" stick across reloads for NOTIFY_SNOOZE_MS.
    notifySnoozedThisSession = true;
    var k = contractSnoozeKey();
    if (!k) return;
    try {
      localStorage.setItem(k, String(Date.now()));
    } catch (e) {}
  }

  // A per-tag throttle (so distinct rooms aren't dropped) plus a rolling global
  // cap (so a consented contract can't flood with unique tags), with a bounded
  // per-tag map so a distinct-tag flood can't grow memory unbounded. `ok(tag,
  // now)` takes the clock as an argument (rather than reading `Date.now()`
  // inside) so the extracted factory is deterministically unit-testable.
  //
  // notify-rate-limiter:BEGIN — self-contained; extracted verbatim between these
  // markers and unit-tested by
  // crates/core/src/server/shell_bridge_notifications.test.mjs. Keep it pure
  // (no reference to anything outside this function) so the extraction works.
  function makeNotifyRateLimiter() {
    var TAG_MIN_MS = 3000; // same-tag throttle window
    var GLOBAL_MAX = 20; // max notifications per rolling window
    var GLOBAL_WINDOW_MS = 60000;
    var MAP_CAP = 128; // bound the per-tag map; evict down to MAP_CAP/2
    var tagTimes = Object.create(null);
    var recent = [];
    return {
      ok: function (tag, now) {
        var last = tagTimes[tag];
        if (last !== undefined && now - last < TAG_MIN_MS) return false;
        recent = recent.filter(function (t) {
          return now - t < GLOBAL_WINDOW_MS;
        });
        if (recent.length >= GLOBAL_MAX) return false;
        tagTimes[tag] = now;
        recent.push(now);
        var keys = Object.keys(tagTimes);
        if (keys.length > MAP_CAP) {
          keys.sort(function (a, b) {
            return tagTimes[a] - tagTimes[b];
          });
          for (var i = 0; i < keys.length - MAP_CAP / 2; i++) {
            delete tagTimes[keys[i]];
          }
        }
        return true;
      },
    };
  }
  // notify-rate-limiter:END

  function notifyStatusToIframe(status) {
    sendToIframe({
      __freenet_shell__: true,
      type: 'notification_status',
      status: status,
    });
  }

  // Show a small, clearly host-owned affordance and, on a REAL click in this
  // shell frame, request the browser permission. The prompt must be triggered
  // by a gesture in the shell: a click inside the sandboxed iframe does not
  // reliably grant the shell the transient activation the browser requires for
  // Notification.requestPermission().
  function maybeOfferNotifications() {
    if (typeof Notification === 'undefined') {
      notifyStatusToIframe('unsupported');
      return;
    }
    if (Notification.permission === 'denied') {
      notifyStatusToIframe('denied');
      return;
    }
    if (Notification.permission === 'granted' && contractHasConsent()) {
      notifyStatusToIframe('granted');
      return;
    }
    // Respect a prior "Not now": makes dismissal effective against a contract
    // that re-sends notification_enable_prompt, and persists for a cooldown.
    if (isNotifySnoozed()) {
      notifyStatusToIframe('dismissed');
      return;
    }
    if (notifyAffordanceShown) return;
    notifyAffordanceShown = true;

    var bar = document.createElement('div');
    bar.setAttribute('role', 'dialog');
    bar.setAttribute('aria-label', 'Enable notifications');
    bar.style.cssText =
      'position:fixed;left:50%;bottom:16px;transform:translateX(-50%);' +
      'z-index:2147483647;display:flex;align-items:center;gap:12px;' +
      'max-width:calc(100% - 32px);padding:10px 14px;border-radius:10px;' +
      'background:#1b1f24;color:#fff;font:14px/1.3 system-ui,sans-serif;' +
      'box-shadow:0 4px 20px rgba(0,0,0,0.35);';
    var label = document.createElement('span');
    label.textContent = 'Get notified of new messages?';
    label.style.cssText = 'flex:1;';
    var enable = document.createElement('button');
    enable.textContent = 'Enable';
    enable.style.cssText =
      'cursor:pointer;border:none;border-radius:6px;padding:6px 12px;' +
      'background:#007FFF;color:#fff;font:inherit;font-weight:600;';
    var dismiss = document.createElement('button');
    dismiss.textContent = 'Not now';
    dismiss.style.cssText =
      'cursor:pointer;border:none;border-radius:6px;padding:6px 10px;' +
      'background:transparent;color:#9aa4b2;font:inherit;';

    function close() {
      notifyAffordanceShown = false;
      try {
        document.body.removeChild(bar);
      } catch (e) {}
    }
    enable.addEventListener('click', function () {
      var called = false;
      var done = function (perm) {
        if (called) return; // some browsers fire BOTH the callback and promise
        called = true;
        if (perm === 'granted' && setContractConsent()) {
          notifyStatusToIframe('granted');
        } else if (perm === 'granted') {
          // Granted but no contract key to gate on — nothing would deliver.
          notifyStatusToIframe('undeliverable');
        } else {
          notifyStatusToIframe(perm === 'denied' ? 'denied' : 'default');
        }
        close();
      };
      try {
        // requestPermission is promise-based on modern browsers and
        // callback-based on older ones — support both. Resolve `done` on
        // rejection too, so a failed prompt never strands the affordance.
        var p = Notification.requestPermission(done);
        if (p && typeof p.then === 'function') {
          p.then(done, function () {
            done('default');
          });
        }
      } catch (e) {
        done('default');
      }
    });
    dismiss.addEventListener('click', function () {
      setNotifySnoozed();
      notifyStatusToIframe('dismissed');
      close();
    });
    bar.appendChild(label);
    bar.appendChild(enable);
    bar.appendChild(dismiss);
    document.body.appendChild(bar);
  }

  function showAppNotification(msg) {
    if (typeof Notification === 'undefined') return;
    // Gate on BOTH the browser permission and this contract's own consent.
    if (Notification.permission !== 'granted' || !contractHasConsent()) return;
    // Notification renders text only (no markup), so no HTML-injection risk;
    // still cap length to prevent oversized/abusive content.
    var title = String(msg.title).slice(0, 128);
    var opts = {};
    if (typeof msg.body === 'string') opts.body = msg.body.slice(0, 256);
    // Coalesce per contract (+ optional app tag) so a busy room replaces rather
    // than stacks notifications; scope the tag to the contract so contracts
    // can't collide.
    var ckey = contractConsentKey() || 'freenet_notify:app';
    opts.tag =
      ckey + ':' + (typeof msg.tag === 'string' ? msg.tag.slice(0, 64) : 'msg');
    // Per-tag throttle + rolling global cap: distinct rooms aren't dropped, but
    // a consented contract can't flood with unique tags.
    if (!notifyLimiter.ok(opts.tag, Date.now())) return;
    var n;
    try {
      n = new Notification(title, opts);
    } catch (e) {
      // e.g. mobile Chrome, where non-persistent `new Notification()` throws
      // (it requires ServiceWorkerRegistration.showNotification). Tell the app
      // so it need not keep sending; the in-app unread badge is the fallback.
      notifyStatusToIframe('undeliverable');
      return;
    }
    n.onclick = function () {
      try {
        window.focus();
      } catch (e) {}
      // Tell the app which notification was clicked so it can route to the room.
      sendToIframe({
        __freenet_shell__: true,
        type: 'notification_click',
        tag: typeof msg.tag === 'string' ? msg.tag : null,
      });
      try {
        n.close();
      } catch (e) {}
    };
  }

  window.addEventListener('message', function (event) {
    if (event.source !== iframe.contentWindow) return;
    var msg = event.data;
    if (!msg) return;

    // Handle shell-level messages (title, favicon) from iframe
    if (msg.__freenet_shell__) {
      if (msg.type === 'title' && typeof msg.title === 'string') {
        // Truncate to prevent UI spoofing with excessively long titles
        document.title = msg.title.slice(0, 128);
      } else if (msg.type === 'favicon' && typeof msg.href === 'string') {
        // Only allow https: and data: schemes to prevent exfiltration
        try {
          var scheme = msg.href.split(':')[0].toLowerCase();
          if (scheme !== 'https' && scheme !== 'data') return;
        } catch (e) {
          return;
        }
        var link = document.querySelector('link[rel="icon"]');
        if (link) link.href = msg.href;
      } else if (msg.type === 'hash' && typeof msg.hash === 'string') {
        // Only allow # fragments — reject anything that could modify path/query.
        // Note: replaceState (not pushState) is intentional — avoids polluting
        // browser history with every in-app route change. This also means
        // replaceState does NOT fire popstate or hashchange, preventing loops.
        var h = msg.hash.slice(0, 8192);
        if (h.length > 0 && h.charAt(0) === '#') {
          // Preserve the existing state object (which may carry our
          // __freenet_nav__ marker) so popstate can still restore the iframe.
          // If the current entry is tagged, also update its iframePath to
          // include the new fragment — otherwise back/forward would restore
          // the iframe without the user's current fragment position.
          var curState = history.state;
          if (
            curState &&
            curState.__freenet_nav__ === true &&
            typeof curState.iframePath === 'string'
          ) {
            var basePath = curState.iframePath.split('#')[0];
            history.replaceState(
              { __freenet_nav__: true, iframePath: basePath + h },
              '',
              h,
            );
          } else {
            history.replaceState(history.state, '', h);
          }
        }
      } else if (msg.type === 'clipboard' && typeof msg.text === 'string') {
        // Sandboxed iframes can't use navigator.clipboard due to permissions
        // policy. Proxy clipboard writes through the trusted shell instead.
        // Write-only — no readText proxy to prevent exfiltration.
        // Rate-limited to 1 write/sec to prevent clipboard spam from
        // malicious contracts. Requires transient user activation (browser
        // enforced) — works when the iframe sends this in a click handler.
        var now = Date.now();
        if (now - lastClipboard >= 1000) {
          lastClipboard = now;
          try {
            navigator.clipboard.writeText(msg.text.slice(0, 2048));
          } catch (e) {}
        }
      } else if (
        msg.type === 'download' &&
        typeof msg.filename === 'string' &&
        typeof msg.base64 === 'string'
      ) {
        // Download proxy: contracts inside the sandboxed (null-origin)
        // iframe can't reliably trigger file downloads — `<a download>`
        // either silently fails (Firefox) or saves to an inaccessible
        // location (Chrome). The shell runs in the real origin and
        // can do it normally.
        //
        // Validation:
        //  - filename: stripped of path separators and leading dots,
        //    capped at 128 chars, no nulls
        //  - mimeType: only a small allowlist (data URLs from arbitrary
        //    types could be exploited by malicious contracts)
        //  - base64: capped at ~10 MiB raw bytes
        //  - rate-limit: 1 download per 2s, same reasoning as clipboard
        var now2 = Date.now();
        if (now2 - lastDownload < 2000) {
          console.warn('[freenet] download rate-limited (>1 per 2s)');
          return;
        }
        // Charge the rate-limit budget for *every* attempt (even rejected
        // ones) so a malicious iframe can't burn host CPU by spamming
        // invalid payloads at high frequency.
        lastDownload = now2;
        var rawName = msg.filename;
        if (rawName.indexOf('\0') !== -1) {
          console.warn('[freenet] download rejected: null byte in filename');
          return;
        }
        // Strip path components — keep only the basename. Normalise
        // both `/` and `\` so a malicious contract can't smuggle in a
        // backslash on POSIX.
        var slash = rawName.lastIndexOf('/');
        if (slash >= 0) rawName = rawName.slice(slash + 1);
        var bslash = rawName.lastIndexOf('\\');
        if (bslash >= 0) rawName = rawName.slice(bslash + 1);
        // Strip leading dots so a contract can't write a dotfile.
        while (rawName.charAt(0) === '.') rawName = rawName.slice(1);
        rawName = rawName.slice(0, 128);
        if (rawName.length === 0) {
          console.warn(
            '[freenet] download rejected: empty filename after sanitisation',
          );
          return;
        }
        var mime =
          typeof msg.mimeType === 'string'
            ? msg.mimeType
            : 'application/octet-stream';
        var ALLOWED_MIME = {
          'application/json': 1,
          'application/octet-stream': 1,
          'text/plain': 1,
          'text/csv': 1,
        };
        // Disallowed MIMEs are downgraded to octet-stream rather than
        // rejected, so callers always get *some* download — but log it
        // so the contract author can fix the mismatch.
        if (!ALLOWED_MIME[mime]) {
          console.warn(
            '[freenet] download MIME ' +
              mime +
              ' downgraded to application/octet-stream',
          );
          mime = 'application/octet-stream';
        }
        // base64 max length ≈ 4/3 * raw size; 10 MiB raw → ~13.4 MiB b64.
        // Round up to 14 MiB for a small safety margin.
        if (msg.base64.length > 14 * 1024 * 1024) {
          console.warn(
            '[freenet] download rejected: payload exceeds 14 MiB base64 cap',
          );
          return;
        }
        var raw;
        try {
          raw = atob(msg.base64);
        } catch (e) {
          console.warn('[freenet] download rejected: base64 decode failed');
          return;
        }
        var len = raw.length;
        var bytes = new Uint8Array(len);
        for (var i = 0; i < len; i++) bytes[i] = raw.charCodeAt(i) & 0xff;
        var blob;
        try {
          blob = new Blob([bytes], { type: mime });
        } catch (e) {
          console.warn('[freenet] download rejected: Blob construction failed');
          return;
        }
        var url;
        try {
          url = URL.createObjectURL(blob);
        } catch (e) {
          console.warn('[freenet] download rejected: createObjectURL failed');
          return;
        }
        var a = document.createElement('a');
        a.href = url;
        a.download = rawName;
        a.style.display = 'none';
        document.body.appendChild(a);
        try {
          a.click();
        } catch (e) {}
        document.body.removeChild(a);
        // Defer revoke so the browser has time to start the download.
        setTimeout(function () {
          try {
            URL.revokeObjectURL(url);
          } catch (e) {}
        }, 60000);
      } else if (msg.type === 'notification_enable_prompt') {
        // The app (opaque origin) can't use the Notifications API itself, so it
        // asks the shell to offer notifications. We show an in-shell affordance
        // and fire the actual permission prompt from a real click in THIS frame
        // (see maybeOfferNotifications for why the gesture must be here).
        maybeOfferNotifications();
      } else if (msg.type === 'notification' && typeof msg.title === 'string') {
        // The app asks the shell to display a browser notification. Gated on
        // the browser permission AND this contract's own stored consent, and
        // rate-limited + length-capped (content is attacker-controlled message
        // text). See showAppNotification.
        showAppNotification(msg);
      } else if (msg.type === 'navigate' && typeof msg.href === 'string') {
        // Navigation from the sandboxed iframe. The iframe cannot navigate
        // the top window itself, so it postMessages the shell, which does
        // one of two things:
        //
        //   1. SAME-CONTRACT hop (subpage inside the current contract's
        //      webapp): update iframe.src in place. This preserves the
        //      running shell, auth token, and in-memory state — matching
        //      what a multi-page webapp expects for client-side routing.
        //
        //   2. CROSS-CONTRACT hop (link to a different Freenet contract):
        //      fall through to a top-level window.location.assign. The
        //      gateway serves a fresh shell via `contract_home` for the
        //      new contract, which generates a new auth token and origin
        //      attribution. Reusing the current iframe for a different
        //      contract would keep the old auth token bound to the
        //      original contract, so the server would misattribute every
        //      subsequent delegate/API request (see PR review: Codex P1).
        //
        // This is the fix for the "Delta cannot link to other Freenet
        // contracts without forcing a new tab" report: cross-contract
        // links now navigate in place via a full shell reload, instead of
        // being silently dropped.
        //
        // Security posture:
        // - Same-origin only (rejects cross-site). The sandbox still
        //   blocks contract JS from reading gateway cookies or same-origin
        //   state.
        // - Target path must match the contract-webapp shape
        //   /v[12]/contract/web/{key}/... . This rejects /v1/node/...,
        //   /v1/delegate/..., or any other gateway endpoint as a
        //   navigation target.
        // - Sandbox iframe attributes are NOT widened. The shell remains
        //   the sole code with top-level navigation authority.
        // - Cross-contract navigation via window.location.assign is the
        //   same privilege level as a user middle-clicking a link today
        //   (target="_blank" + allow-popups already escapes the sandbox
        //   and can reach any Freenet contract). The difference is that
        //   the destination now loads in the same tab instead of a new
        //   one.
        //
        // Cap href length to prevent a malicious contract from bloating
        // history.state or the address bar with arbitrarily large URLs.
        if (msg.href.length > 4096) return;
        try {
          var resolved = new URL(msg.href, iframe.src);
          // Same-origin only.
          if (resolved.origin !== location.origin) return;
          var cleanPath = resolved.pathname;
          // Contract-webapp shape check. This is the security boundary
          // that prevents the handler from being used to navigate to
          // gateway internals (/v1/node/..., /v1/delegate/...) or to
          // non-contract paths in general. The contract-key segment is
          // validated server-side in the freshly-loaded shell path via
          // ContractInstanceId::from_bytes, so we only need a loose
          // shape check here — a bogus key still produces a 4xx from the
          // gateway, not a silent bypass.
          var newPrefixMatch = cleanPath.match(CONTRACT_PREFIX_RE);
          if (!newPrefixMatch) return;
          var newContractPrefix = newPrefixMatch[1];
          // Cap the hash component to match the 8192-byte cap used by
          // the hash-forwarding path; the iframe path is stored in
          // history.state so unbounded hashes would bloat the per-tab
          // history record.
          var cappedHash = resolved.hash ? resolved.hash.slice(0, 8192) : '';

          if (newContractPrefix === contractPrefix) {
            // SAME-CONTRACT: update iframe.src in place. This preserves
            // the running shell, auth token, and client-side state.
            //
            // Close any open WebSocket connections from the previous
            // page to prevent resource leaks. The old iframe document
            // will be destroyed when src changes, orphaning any
            // connection callbacks.
            connections.forEach(function (ws) {
              try {
                ws.close();
              } catch (e) {}
            });
            connections.clear();
            // Build new sandbox URL preserving __sandbox=1
            resolved.searchParams.set('__sandbox', '1');
            var newIframePath =
              resolved.pathname + resolved.search + cappedHash;
            iframe.src = newIframePath;
            // Push a history entry so back/forward navigate between
            // visited subpages, and update the address bar to the
            // non-sandbox URL. The sandbox flag is intentionally omitted
            // from the outer URL; the shell always re-adds it when
            // loading the iframe. See issue #3839.
            try {
              history.pushState(
                { __freenet_nav__: true, iframePath: newIframePath },
                '',
                cleanPath + cappedHash,
              );
            } catch (e) {}
          } else {
            // CROSS-CONTRACT: top-level navigation. The gateway's
            // contract_home handler re-runs and generates a fresh auth
            // token + origin attribution for the destination contract.
            // The browser's normal back/forward history takes care of
            // cross-contract restoration — no popstate handling needed.
            //
            // Include `resolved.search` so any query parameters the link
            // carries (e.g. app-level routing args) survive the hop. The
            // destination shell page strips the sensitive routing params
            // (`__sandbox`, `authToken`) before forwarding the rest into
            // the iframe's `location.search`. The gateway's subpage
            // handler redirects non-root HTML loads to the shell route
            // (see `web_subpages` `Sec-Fetch-Dest` handling), which
            // preserves the filtered query string all the way through,
            // so `/v1/contract/web/{key}/page2?invite=…` still lands on
            // a shell that issues an auth token and forwards `invite`
            // into the iframe.
            try {
              window.location.assign(cleanPath + resolved.search + cappedHash);
            } catch (e) {}
          }
        } catch (e) {}
      } else if (msg.type === 'open_url' && typeof msg.url === 'string') {
        // Open external URLs in a new tab. Popups from the sandboxed iframe
        // inherit the opaque origin, breaking CORS on target sites. The shell
        // opens the URL instead, giving proper origin. See issue #1499.
        //
        // Security model: this scheme allow-list is the PRIMARY gate, not
        // defence in depth. A malicious contract iframe can postMessage
        // `open_url` directly without going through the upstream
        // navigation interceptor, so the URL parser + scheme check below
        // is what blocks `javascript:` / `data:` / `file:` etc.
        //
        // Both http and https are accepted because user-pasted markdown
        // links commonly target plain-HTTP self-hosted services (e.g.
        // nova.locut.us:3133, the Freenet network telemetry dashboard,
        // no TLS configured). Auth tokens never travel through this path
        // — the only operation is `window.open(url, '_blank',
        // 'noopener,noreferrer')` — so HTTP doesn't expose credentials.
        // See freenet/river#231.
        //
        // Private networks (RFC1918 192.168/16, 10/8, 172.16-31/12 and
        // RFC4193 fc00::/7, link-local fe80::/10) are deliberately NOT
        // blocked. A user who pastes a link to their home router or NAS
        // expects the link to work; the threat model here is that a
        // *malicious contract* might forge a markdown link to a LAN
        // admin panel and trick the user into clicking, which is a
        // social-engineering attack class we accept.
        try {
          var u = new URL(msg.url);
          if (u.protocol !== 'https:' && u.protocol !== 'http:') return;
          // URL.hostname strips brackets from IPv6 literals, so a URL
          // `http://[::1]/` parses with hostname `::1`, NOT `[::1]`.
          // Compare against the bracket-less form.
          var h = u.hostname.toLowerCase();
          if (
            h === 'localhost' ||
            h === '127.0.0.1' ||
            h === '::1' ||
            h === '0.0.0.0'
          )
            return;
          // Honour shift-click by requesting a popup-style window feature
          // (freenet/freenet-core#3853). Firefox honours this as "open in
          // a new window"; other browsers may still open a tab, which is
          // an acceptable fallback. ctrl / meta / middle-click cannot be
          // preserved from a postMessage handler because browsers only
          // honour background-tab placement when window.open is called
          // from a direct user gesture, so we route those through the
          // same default-tab path as plain left-click.
          if (msg.shiftKey === true) {
            window.open(u.href, '_blank', 'noopener,noreferrer,popup');
          } else {
            window.open(u.href, '_blank', 'noopener,noreferrer');
          }
        } catch (e) {}
      }
      return;
    }

    if (!msg.__freenet_ws__) return;

    switch (msg.type) {
      case 'open': {
        // FAIL CLOSED (#4381): a hosted browser with no per-user token must
        // never open a socket, because that connection would land on the
        // SHARED Local namespace (cross-user contamination). The token is
        // absent over plaintext http (withheld) or on a storage/crypto
        // failure. Refuse every open while hosted+no-token — a second
        // independent barrier on top of not loading the iframe at all (see the
        // hostedNoToken block at the top of freenetBridge).
        if (hostedNoToken) {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          return;
        }
        // Limit concurrent connections to prevent resource exhaustion
        if (connections.size >= MAX_CONNECTIONS) {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          return;
        }
        // Security: only allow WebSocket connections to the local API server itself.
        // Validate protocol explicitly and compare origin.
        try {
          var u = new URL(msg.url);
          if (u.protocol !== 'ws:' && u.protocol !== 'wss:') {
            sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
            return;
          }
          var httpProto = u.protocol === 'wss:' ? 'https:' : 'http:';
          if (httpProto + '//' + u.host !== LOCAL_API_ORIGIN) {
            sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
            return;
          }
        } catch (e) {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          return;
        }
        // Strip any caller-supplied credentials BEFORE we inject our own, so
        // the sandboxed app can never choose its own auth/user identity by
        // putting these params on the WebSocket URL it asks us to open. This
        // matters most for `userToken`: the conditional `set` below is skipped
        // when our minted token is undefined (localStorage disabled / private
        // mode in hosted mode), and without this delete a caller-supplied
        // `userToken` (from the app, or reflected via a deep-link into the WS
        // URL) would survive and let the app pick its own per-user secret
        // namespace. The app must NEVER influence the namespace — only the
        // shell-minted token may reach the backend. `authToken` is deleted for
        // symmetric defense-in-depth even though the unconditional `set` below
        // already overrides any caller value.
        u.searchParams.delete('authToken');
        u.searchParams.delete('userToken');
        // Inject auth token into the WebSocket URL
        u.searchParams.set('authToken', authToken);
        // In hosted mode also present the durable per-user token so the node
        // can scope a per-user delegate-secret namespace (P2 of #4381). The
        // token is undefined in non-hosted mode, so the param is omitted (and,
        // combined with the delete above, the URL carries no userToken at all).
        // The `location.protocol === 'https:'` guard is a SECOND, independent
        // REFUSE-PLAINTEXT-TOKEN barrier (the first is in SHELL_USER_TOKEN_JS,
        // which never mints the token on an http page): two guards so a future
        // refactor of either site can't reopen the plaintext-leak path. The
        // shell is same-origin with the node, so `location.protocol` reflects
        // whether the shell itself was served over TLS.
        if (userToken && location.protocol === 'https:') {
          u.searchParams.set('userToken', userToken);
        }
        var ws = new WebSocket(u.toString(), msg.protocols || undefined);
        ws.binaryType = 'arraybuffer';
        connections.set(msg.id, ws);

        ws.onopen = function () {
          sendToIframe({ __freenet_ws__: true, type: 'open', id: msg.id });
        };
        ws.onmessage = function (e) {
          var transfer = e.data instanceof ArrayBuffer ? [e.data] : [];
          iframe.contentWindow.postMessage(
            {
              __freenet_ws__: true,
              type: 'message',
              id: msg.id,
              data: e.data,
            },
            '*',
            transfer,
          );
        };
        ws.onclose = function (e) {
          sendToIframe({
            __freenet_ws__: true,
            type: 'close',
            id: msg.id,
            code: e.code,
            reason: e.reason,
          });
          connections.delete(msg.id);
        };
        ws.onerror = function () {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          connections.delete(msg.id);
        };
        break;
      }
      case 'send': {
        var ws = connections.get(msg.id);
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(msg.data);
        }
        break;
      }
      case 'close': {
        var ws = connections.get(msg.id);
        if (ws) {
          ws.close(msg.code, msg.reason);
          connections.delete(msg.id);
        }
        break;
      }
    }
  });

  // Forward runtime hash changes (browser back/forward, manual URL edits)
  function forwardHash() {
    if (location.hash) {
      sendToIframe({
        __freenet_shell__: true,
        type: 'hash',
        hash: location.hash.slice(0, 8192),
      });
    }
  }
  // popstate fires when the user presses back/forward. If the popped entry
  // carries our __freenet_nav__ marker, restore the iframe to the matching
  // subpage. Otherwise, fall back to forwarding the hash. See issue #3839.
  window.addEventListener('popstate', function (ev) {
    var state = ev.state;
    if (
      state &&
      state.__freenet_nav__ === true &&
      typeof state.iframePath === 'string'
    ) {
      // Security: path must still live under this contract's web prefix.
      // A stale state object from a different contract must not be able to
      // redirect the iframe elsewhere.
      if (contractPrefix && state.iframePath.indexOf(contractPrefix) === 0) {
        // No-op if the iframe is already on the target path (e.g. popstate
        // fired from a bfcache restore where iframe state was retained).
        // This avoids a spurious reload that would tear down live WebSocket
        // connections unnecessarily.
        if (iframe.src.indexOf(state.iframePath) === -1) {
          connections.forEach(function (ws) {
            try {
              ws.close();
            } catch (e) {}
          });
          connections.clear();
          iframe.src = state.iframePath;
        }
        return;
      }
    }
    forwardHash();
  });
  window.addEventListener('hashchange', forwardHash);

  // Permission prompt overlay: render a modal in the shell page's DOM
  // (outside the sandboxed iframe) whenever a delegate permission prompt
  // is pending. The shell is trusted and same-origin with the gateway, so
  // the sandboxed contract cannot reach into this DOM. See issue #3836.
  //
  // Every open Freenet tab subscribes to /permission/events (Server-Sent
  // Events) and renders the overlay as soon as the gateway pushes an
  // `prompt_added` event. When the user responds in one tab, the gateway
  // emits `prompt_removed` and every tab dismisses its card. This was
  // previously a 3-second polling loop with a visibility-skip optimisation
  // that caused the originating tab to silently miss prompts whenever it
  // wasn't foregrounded; SSE eliminates both the polling-floor latency and
  // the visibility race.
  var overlayRoot = null;
  var overlayCards = {}; // nonce -> card element
  var OVERLAY_CSS =
    '#__freenet_perm_overlay{position:fixed;inset:0;z-index:2147483647;' +
    'background:rgba(8,10,14,0.62);backdrop-filter:blur(4px);' +
    '-webkit-backdrop-filter:blur(4px);display:none;align-items:center;' +
    'justify-content:center;padding:20px;overflow:auto;' +
    'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;}' +
    '#__freenet_perm_overlay .fn-card{--bg:#0f1419;--fg:#e6e8eb;--card:#1a2028;' +
    '--accent:#3b82f6;--border:#2d3748;--warn:#f59e0b;--muted:#9ca3af;' +
    'background:var(--card);color:var(--fg);border:1px solid var(--border);' +
    'border-radius:14px;padding:28px;max-width:520px;width:100%;margin:12px 0;' +
    'box-shadow:0 12px 40px rgba(0,0,0,0.5);box-sizing:border-box;}' +
    '@media (prefers-color-scheme: light){#__freenet_perm_overlay .fn-card{' +
    '--bg:#f5f5f5;--fg:#1a1a1a;--card:#ffffff;--accent:#2563eb;' +
    '--border:#d1d5db;--warn:#d97706;--muted:#6b7280;' +
    'box-shadow:0 12px 40px rgba(0,0,0,0.18);}}' +
    '#__freenet_perm_overlay .fn-header{display:flex;align-items:center;gap:12px;' +
    'margin-bottom:18px;}' +
    '#__freenet_perm_overlay .fn-icon{font-size:28px;line-height:1;}' +
    '#__freenet_perm_overlay .fn-title{font-size:18px;font-weight:600;margin:0;' +
    'color:var(--fg);}' +
    '#__freenet_perm_overlay .fn-msg-label{font-size:11px;color:var(--muted);' +
    'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;}' +
    '#__freenet_perm_overlay .fn-msg{font-size:15px;line-height:1.5;margin:0 0 22px 0;' +
    'padding:14px 16px;background:var(--bg);border-left:3px solid var(--warn);' +
    'border-radius:4px;white-space:pre-wrap;word-wrap:break-word;color:var(--fg);}' +
    '#__freenet_perm_overlay .fn-msg-pre{font-family:ui-monospace,SFMono-Regular,' +
    'Menlo,Monaco,Consolas,monospace;font-size:12px;line-height:1.45;' +
    'max-height:300px;overflow:auto;}' +
    '#__freenet_perm_overlay .fn-btns{display:flex;gap:10px;flex-wrap:wrap;}' +
    '#__freenet_perm_overlay .fn-btn{padding:10px 20px;border-radius:8px;' +
    'font-size:14px;cursor:pointer;flex:1;min-width:100px;font-weight:500;' +
    'border:1px solid var(--border);background:var(--card);color:var(--fg);' +
    'transition:transform 0.12s, opacity 0.12s, filter 0.12s;font-family:inherit;}' +
    '#__freenet_perm_overlay .fn-btn.primary{background:var(--accent);' +
    'color:#fff;border-color:var(--accent);}' +
    '#__freenet_perm_overlay .fn-btn:hover:not(:disabled){transform:translateY(-1px);' +
    'filter:brightness(1.08);}' +
    '#__freenet_perm_overlay .fn-btn:disabled{opacity:0.55;cursor:not-allowed;}' +
    '#__freenet_perm_overlay .fn-delegate-line{font-size:12px;color:var(--muted);' +
    'margin-top:10px;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;}' +
    '#__freenet_perm_overlay .fn-delegate-line .hash{user-select:all;}' +
    '#__freenet_perm_overlay .fn-tech{margin-top:10px;font-size:12px;color:var(--muted);}' +
    '#__freenet_perm_overlay .fn-tech summary{cursor:pointer;user-select:none;}' +
    '#__freenet_perm_overlay .fn-tech dl{margin:8px 0 0 16px;}' +
    '#__freenet_perm_overlay .fn-tech dt{font-weight:600;color:var(--fg);margin-top:6px;}' +
    '#__freenet_perm_overlay .fn-tech dd{margin:2px 0 0 0;font-family:ui-monospace,' +
    'SFMono-Regular,Menlo,Consolas,monospace;word-break:break-all;user-select:all;}' +
    '#__freenet_perm_overlay .fn-timer{margin-top:14px;font-size:12px;' +
    'color:var(--muted);text-align:center;}';
  // Auto-deny duration in seconds, mirroring the standalone /permission/{nonce}
  // fallback page. Tracked client-side only; the server enforces the real
  // timeout and will clear the nonce regardless.
  var OVERLAY_AUTO_DENY_SECONDS = 60;
  function ensureOverlayRoot() {
    if (overlayRoot) return overlayRoot;
    var style = document.createElement('style');
    style.textContent = OVERLAY_CSS;
    document.head.appendChild(style);
    overlayRoot = document.createElement('div');
    overlayRoot.id = '__freenet_perm_overlay';
    overlayRoot.setAttribute('role', 'dialog');
    overlayRoot.setAttribute('aria-modal', 'true');
    overlayRoot.setAttribute('aria-label', 'Delegate permission request');
    document.body.appendChild(overlayRoot);
    // Escape-to-dismiss: routes to the last button in the most-recently-added
    // card, which (by the standard delegate convention Allow Once / Always
    // Allow / Deny) is the Deny button. If the delegate supplied a single
    // label this is a no-op — Escape just does nothing.
    document.addEventListener('keydown', function (e) {
      if (e.key !== 'Escape') return;
      if (!overlayRoot || overlayRoot.style.display === 'none') return;
      var nonces = Object.keys(overlayCards);
      if (nonces.length === 0) return;
      var nonce = nonces[nonces.length - 1];
      var card = overlayCards[nonce];
      var btns = card.querySelectorAll('button');
      if (btns.length < 2) return; // no non-primary option, ignore
      btns[btns.length - 1].click();
      e.preventDefault();
    });
    return overlayRoot;
  }
  function setText(el, text) {
    // textContent avoids any HTML interpretation of delegate-controlled
    // strings. Delegate-provided fields are never parsed as markup.
    el.textContent = text == null ? '' : String(text);
  }
  // Truncate a hash for display: first8…last5. Mirrors truncate_hash() in
  // crates/core/src/server/client_api/permission_prompts.rs so the overlay
  // and the standalone /permission/{nonce} fallback page render identically.
  // Handles multi-byte unicode by iterating Array.from(...) which gives
  // codepoints, not UTF-16 code units.
  function truncateHash(s) {
    if (typeof s !== 'string' || s.length === 0) return '';
    var chars = Array.from(s);
    if (chars.length <= 14) return s;
    return (
      chars.slice(0, 8).join('') +
      '\u2026' +
      chars.slice(chars.length - 5).join('')
    );
  }
  // Render the Caller row from the tagged caller object. Forward-compatible:
  // an unknown `kind` (e.g. a future "delegate" variant from issue #3860)
  // falls through to a neutral "Unknown caller" so the overlay does NOT
  // pretend to render an identity it doesn't understand.
  function formatCaller(caller) {
    if (!caller || typeof caller !== 'object') {
      return { display: 'No app caller', full: '' };
    }
    if (caller.kind === 'webapp' && typeof caller.hash === 'string') {
      return {
        display: 'Freenet app ' + truncateHash(caller.hash),
        full: caller.hash,
      };
    }
    if (caller.kind === 'none') {
      return { display: 'No app caller', full: '' };
    }
    return { display: 'Unknown caller', full: '' };
  }
  function createCard(p) {
    var card = document.createElement('div');
    card.className = 'fn-card';
    card.setAttribute('data-nonce', p.nonce);

    var header = document.createElement('div');
    header.className = 'fn-header';
    var icon = document.createElement('span');
    icon.className = 'fn-icon';
    icon.textContent = '\u{1F512}';
    var title = document.createElement('h1');
    title.className = 'fn-title';
    title.textContent = 'Permission Request';
    header.appendChild(icon);
    header.appendChild(title);
    card.appendChild(header);

    // "Delegate says:" authorship label is non-negotiable: a malicious
    // delegate would otherwise be able to write text like "Freenet verified
    // this request" with no way for the user to tell who authored it. The
    // text below the label is delegate-controlled; the label tells the user
    // that. See the trust-model rationale in permission_prompts.rs.
    var msgLabel = document.createElement('div');
    msgLabel.className = 'fn-msg-label';
    msgLabel.textContent = 'Delegate says:';
    card.appendChild(msgLabel);
    // Try to render the delegate-supplied message as pretty-printed JSON
    // when it parses as JSON. Falls back to a plain paragraph for plain
    // text. The pretty form makes structured token requests legible
    // (#190) — users routinely see one-line blobs like
    //   {"token":{"max_age":"31536000 seconds","tier":"Min10"},...}
    // and have to mentally parse them to make a security decision.
    //
    // Security: still rendered via textContent (setText), so no HTML
    // interpretation. Long values are wrapped via CSS (white-space:
    // pre-wrap on .fn-msg-pre). Render is best-effort: any parse error
    // falls back to the original raw string in a <p>.
    var rawMsg = p.message || 'A delegate is requesting permission.';
    var pretty = null;
    if (typeof rawMsg === 'string' && rawMsg.length > 0) {
      var trimmed = rawMsg.trim();
      if (
        trimmed.length <= 64 * 1024 &&
        (trimmed.charAt(0) === '{' || trimmed.charAt(0) === '[')
      ) {
        try {
          var parsed = JSON.parse(trimmed);
          pretty = JSON.stringify(parsed, null, 2);
          // Cap rendered output at 16 KiB after pretty-printing so a
          // hostile delegate can't force a multi-MiB layout pass.
          if (pretty.length > 16 * 1024) {
            pretty = pretty.slice(0, 16 * 1024) + '\n…';
          }
        } catch (e) {
          pretty = null;
        }
      }
    }
    if (pretty !== null) {
      var msgPre = document.createElement('pre');
      msgPre.className = 'fn-msg fn-msg-pre';
      setText(msgPre, pretty);
      card.appendChild(msgPre);
    } else {
      var msg = document.createElement('p');
      msg.className = 'fn-msg';
      setText(msg, rawMsg);
      card.appendChild(msg);
    }

    var buttons = document.createElement('div');
    buttons.className = 'fn-btns';
    var labels =
      Array.isArray(p.labels) && p.labels.length > 0 ? p.labels : ['OK'];
    labels.forEach(function (label, idx) {
      var b = document.createElement('button');
      b.className = 'fn-btn' + (idx === 0 ? ' primary' : '');
      setText(b, label);
      b.addEventListener('click', function () {
        respondToPrompt(p.nonce, idx, card);
      });
      buttons.appendChild(b);
    });
    card.appendChild(buttons);

    // Inline truncated delegate hash, always visible. Gives the user a
    // passive anomaly signal: a returning user who recognises their
    // delegate's fingerprint can spot an impostor without expanding the
    // Technical details disclosure. Full hash is in the Technical details
    // pane below and copyable via user-select: all on .hash.
    var delegateLine = document.createElement('div');
    delegateLine.className = 'fn-delegate-line';
    var delegateLabel = document.createElement('span');
    delegateLabel.textContent = 'Delegate: ';
    delegateLine.appendChild(delegateLabel);
    var delegateHashSpan = document.createElement('span');
    delegateHashSpan.className = 'hash';
    var delegateFull = typeof p.delegate_key === 'string' ? p.delegate_key : '';
    setText(delegateHashSpan, truncateHash(delegateFull) || '(none)');
    if (delegateFull) {
      delegateHashSpan.setAttribute('title', delegateFull);
    }
    delegateLine.appendChild(delegateHashSpan);
    card.appendChild(delegateLine);

    // Technical details disclosure. Holds the full delegate hash and the
    // Caller row. Closed by default — the user's decision is timing/intent
    // ("did I just trigger this?"), not hash matching. Power users hover or
    // copy via user-select: all to audit the unabbreviated value.
    var details = document.createElement('details');
    details.className = 'fn-tech';
    var summary = document.createElement('summary');
    summary.textContent = 'Technical details';
    details.appendChild(summary);
    var dl = document.createElement('dl');
    var dtDelegate = document.createElement('dt');
    dtDelegate.textContent = 'Delegate';
    var ddDelegate = document.createElement('dd');
    setText(ddDelegate, delegateFull || '(none)');
    if (delegateFull) {
      ddDelegate.setAttribute('title', delegateFull);
    }
    var dtCaller = document.createElement('dt');
    dtCaller.textContent = 'Caller';
    var ddCaller = document.createElement('dd');
    var callerRendered = formatCaller(p.caller);
    setText(ddCaller, callerRendered.display);
    if (callerRendered.full) {
      ddCaller.setAttribute('title', callerRendered.full);
    }
    dl.appendChild(dtDelegate);
    dl.appendChild(ddDelegate);
    dl.appendChild(dtCaller);
    dl.appendChild(ddCaller);
    details.appendChild(dl);
    card.appendChild(details);

    // Countdown mirroring the standalone permission page. The real timeout
    // lives server-side; this is a hint for the user that the prompt won't
    // wait forever. On expiry the next poll drops the card via the
    // reconciliation path, so we don't need a local hide here.
    var timer = document.createElement('div');
    timer.className = 'fn-timer';
    var remaining = OVERLAY_AUTO_DENY_SECONDS;
    timer.textContent = 'Auto-deny in ' + remaining + 's';
    card._fnTimerId = setInterval(function () {
      remaining -= 1;
      if (remaining <= 0) {
        clearInterval(card._fnTimerId);
        timer.textContent = 'Auto-denied';
        return;
      }
      timer.textContent = 'Auto-deny in ' + remaining + 's';
    }, 1000);
    card.appendChild(timer);
    return card;
  }
  function showCard(nonce, card) {
    var root = ensureOverlayRoot();
    root.appendChild(card);
    root.style.display = 'flex';
    overlayCards[nonce] = card;
    // Move keyboard focus to the primary button so Enter/Space answer the
    // prompt without requiring a mouse click.
    var primary = card.querySelector('.fn-btn.primary');
    if (primary && typeof primary.focus === 'function') {
      try {
        primary.focus();
      } catch (e) {}
    }
  }
  function hideCard(nonce) {
    var card = overlayCards[nonce];
    if (!card) return;
    if (card._fnTimerId) {
      clearInterval(card._fnTimerId);
      card._fnTimerId = null;
    }
    if (card.parentNode) card.parentNode.removeChild(card);
    delete overlayCards[nonce];
    if (overlayRoot && Object.keys(overlayCards).length === 0) {
      overlayRoot.style.display = 'none';
    }
  }
  function respondToPrompt(nonce, index, card) {
    var btns = card.querySelectorAll('button');
    btns.forEach(function (b) {
      b.disabled = true;
      b.style.opacity = '0.5';
    });
    fetch('/permission/' + encodeURIComponent(nonce) + '/respond', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ index: index }),
    })
      .then(function (r) {
        // 404 means another tab already answered (or it auto-denied) — hide
        // the overlay here as well so the user isn't staring at a dead button.
        if (r.ok || r.status === 404) {
          hideCard(nonce);
        } else {
          btns.forEach(function (b) {
            b.disabled = false;
            b.style.opacity = '1';
          });
        }
      })
      .catch(function () {
        btns.forEach(function (b) {
          b.disabled = false;
          b.style.opacity = '1';
        });
      });
  }
  // Snapshot the current pending list and reconcile against the open
  // overlay cards. Used for initial bootstrap, on `resync` events when an
  // SSE subscriber lagged, and as a fallback while the EventSource is
  // reconnecting.
  function reconcileFromPending() {
    fetch('/permission/pending')
      .then(function (r) {
        return r.json();
      })
      .then(function (prompts) {
        if (!Array.isArray(prompts)) return;
        var seen = {};
        prompts.forEach(function (p) {
          if (!p || typeof p.nonce !== 'string') return;
          seen[p.nonce] = true;
          if (overlayCards[p.nonce]) return;
          showCard(p.nonce, createCard(p));
        });
        Object.keys(overlayCards).forEach(function (nonce) {
          if (!seen[nonce]) hideCard(nonce);
        });
      })
      .catch(function () {});
  }

  // Open a Server-Sent Events connection so prompts appear with no polling
  // delay and on every open Freenet tab regardless of foreground/background
  // state. The browser's EventSource auto-reconnects with exponential
  // backoff if the connection drops; on each reconnect we re-bootstrap from
  // /permission/pending so we don't miss anything during the gap.
  //
  // While the EventSource is in the disconnected state (its `error` event
  // has fired and `readyState !== 1`), we run a 3-second polling fallback
  // against /permission/pending so a tab whose stream fails (gateway
  // restart, connection-cap rejection, transient network) still receives
  // prompt updates. The fallback shuts off as soon as the stream re-opens.
  var fallbackPollHandle = null;
  function startFallbackPoll() {
    if (fallbackPollHandle !== null) return;
    fallbackPollHandle = setInterval(reconcileFromPending, 3000);
    reconcileFromPending();
  }
  function stopFallbackPoll() {
    if (fallbackPollHandle === null) return;
    clearInterval(fallbackPollHandle);
    fallbackPollHandle = null;
  }
  if (typeof EventSource !== 'undefined') {
    var es = new EventSource('/permission/events');
    es.addEventListener('prompt_added', function (e) {
      try {
        var p = JSON.parse(e.data);
        if (!p || typeof p.nonce !== 'string') return;
        if (overlayCards[p.nonce]) return;
        showCard(p.nonce, createCard(p));
      } catch (err) {}
    });
    es.addEventListener('prompt_removed', function (e) {
      try {
        var p = JSON.parse(e.data);
        if (!p || typeof p.nonce !== 'string') return;
        hideCard(p.nonce);
      } catch (err) {}
    });
    // The server emits `resync` when its broadcast channel laps a slow
    // subscriber. Reconcile from the polling endpoint instead of clearing
    // first: the reconcile path's diff already adds new cards and hides
    // ones that disappeared, with no flicker on cards that survive.
    es.addEventListener('resync', reconcileFromPending);
    // EventSource fires `open` on initial connect AND on every reconnect.
    // Reconcile each time so a transient disconnect doesn't leave us out
    // of date, and stop the fallback poll if it had taken over.
    es.addEventListener('open', function () {
      stopFallbackPoll();
      reconcileFromPending();
    });
    // `error` fires on connect failure, transient drops, and when the
    // server caps us out. Switch to polling until the EventSource
    // re-opens; the browser auto-reconnects in the background.
    es.addEventListener('error', startFallbackPoll);
    // Initial bootstrap so we're populated before the SSE handshake
    // completes (avoids a brief empty state on slow connections).
    reconcileFromPending();
  } else {
    // EventSource missing in some embedded webviews -- fall back to the
    // legacy 3-second poll so users on those clients still see prompts.
    startFallbackPoll();
  }
}
