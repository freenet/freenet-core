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
