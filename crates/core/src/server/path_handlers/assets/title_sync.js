(function () {
  'use strict';
  // Forwards this document's <title> to the shell via the same
  // `__freenet_shell__` / `type: 'title'` postMessage SHELL_BRIDGE_JS already
  // handles. The shell page's own <title> is hardcoded (shell.html) because
  // this iframe has no `allow-same-origin` and cannot touch the parent's
  // `document.title` directly — this postMessage is the ONLY way a contract's
  // title ever reaches the tab.
  //
  // Injected into every sandboxed page unconditionally, so a contract gets a
  // correct tab title with zero opt-in — including a static, JS-free website.
  // A few apps (River, Atlas, Delta) also send this themselves for finer
  // control (e.g. an unread-count suffix); sending it again here is harmless,
  // since the shell just re-assigns document.title to the same string.
  var lastSent = null;
  function sendTitle() {
    var title = document.title;
    // A page with no <title> at all (document.title === '') is left alone
    // rather than resetting the tab: on a multi-page contract site reached
    // via an in-place navigate hop, this means the tab keeps showing the
    // PREVIOUS page's real title rather than reverting to "Freenet". That's
    // a deliberate choice, not an oversight — a stale-but-real title is a
    // narrower, already-strictly-better-than-baseline edge case (before this
    // script existed the tab showed "Freenet" unconditionally, always), and
    // "what should an untitled page show instead" is a separate product
    // decision this fix doesn't make unilaterally.
    if (!title || title === lastSent) return;
    lastSent = title;
    window.parent.postMessage(
      { __freenet_shell__: true, type: 'title', title: title },
      '*',
    );
  }
  sendTitle();
  // Catches a <title> set by an inline/deferred script after this runs, and
  // any later change (SPA route change, async data load). Observing `head`
  // rather than the <title> element itself also catches the element not
  // existing yet at injection time — `document.title = 'x'` on a page with no
  // <title> tag creates one, which this still sees via the head subtree.
  var head = document.head || document.getElementsByTagName('head')[0];
  if (head && window.MutationObserver) {
    new MutationObserver(sendTitle).observe(head, {
      childList: true,
      subtree: true,
      characterData: true,
    });
  }
  // Belt-and-suspenders in case a browser yields to this script before head
  // parsing (and its <title>) has finished.
  document.addEventListener('DOMContentLoaded', sendTitle);
})();
