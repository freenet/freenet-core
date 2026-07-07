(function () {
  var acct = document.getElementById('fnacct');
  var pop = document.getElementById('fnpop');
  var ok = document.getElementById('fnok');
  function setOk(m) {
    ok.textContent = m;
    setTimeout(function () {
      if (ok.textContent === m) ok.textContent = '';
    }, 2500);
  }
  document.getElementById('fnacctbtn').addEventListener('click', function (e) {
    e.stopPropagation();
    pop.classList.toggle('open');
  });
  document.addEventListener('click', function (e) {
    if (!acct.contains(e.target)) pop.classList.remove('open');
  });
  // Clicking into the sandboxed iframe (the app, which fills most of the page)
  // does NOT fire the document click above — the click lands in the iframe's
  // own document. It does blur the shell window, so dismiss the popover on
  // focus loss too, otherwise it stays open until you click the bar again.
  window.addEventListener('blur', function () {
    pop.classList.remove('open');
  });
  document.getElementById('fncopy').addEventListener('click', function () {
    var t =
      typeof __freenet_user_token !== 'undefined' ? __freenet_user_token : null;
    if (!t) {
      setOk('No key on this connection');
      return;
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(t).then(
        function () {
          setOk('Copied to clipboard');
        },
        function () {
          window.prompt('Copy your access key:', t);
        },
      );
    } else {
      window.prompt('Copy your access key:', t);
    }
  });
  document.getElementById('fnrestore').addEventListener('click', function () {
    var v = window.prompt(
      'Paste your saved access key to restore access to your data:',
    );
    if (!v) {
      return;
    }
    v = v.trim();
    if (!v) {
      return;
    }
    try {
      localStorage.setItem('__freenet_user_token__', v);
      location.reload();
    } catch (e) {
      setOk('Storage unavailable');
    }
  });
  document.getElementById('fnnewid').addEventListener('click', function () {
    // Start over with a fresh identity. Deleting the stored token is enough:
    // on reload SHELL_USER_TOKEN_JS mints a new random token because the key is
    // absent. Destructive (the old data on this server is unreachable without
    // the old key), so confirm first and point the user at "Copy key".
    if (
      !window.confirm(
        'Start over with a new identity? Your current data on this server ' +
          'stays under your old access key — copy that key first if you want ' +
          'to get back to it. This browser will switch to a fresh, empty ID.',
      )
    ) {
      return;
    }
    try {
      localStorage.removeItem('__freenet_user_token__');
      location.reload();
    } catch (e) {
      setOk('Storage unavailable');
    }
  });
  document.getElementById('fnexport').addEventListener('click', function () {
    var t =
      typeof __freenet_user_token !== 'undefined' ? __freenet_user_token : null;
    if (!t) {
      setOk('No key on this connection');
      return;
    }
    setOk('Preparing download...');
    // Read the token from the shell-only global and send it in the header the
    // export endpoint requires (never a query param, so it stays out of logs).
    // fetch->blob->download because a plain navigation cannot set the header.
    fetch('/v1/hosted/export', { headers: { 'X-Freenet-User-Token': t } })
      .then(function (r) {
        if (!r.ok) {
          throw new Error('HTTP ' + r.status);
        }
        return r.blob();
      })
      .then(function (blob) {
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = 'freenet-data.fnsx';
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
        setOk('Downloaded freenet-data.fnsx');
      })
      .catch(function (e) {
        setOk('Export failed (' + e.message + ')');
      });
  });

  // "Move to my peer" — the zero-friction magic-link migration (#4592). Mints a
  // one-time pull token on this hosted node (authorized by the shell-only user
  // token) and builds a link the user opens on their OWN peer, which then pulls
  // the data over HTTPS and imports it. The durable access key never leaves this
  // browser: the bundle is sealed under a fresh ephemeral key held server-side.
  var migrateOut = document.getElementById('fnmigrateout');
  var migrateLink = document.getElementById('fnmigratelink');
  var migrateMsg = document.getElementById('fnmigratemsg');
  // Default local-peer web/ws-api port (the freenet config default, see
  // config.rs default_ws_api_port). Overriding a non-default local port is a
  // future refinement.
  var LOCAL_PEER_PORT = 7509;

  function setMigrateMsg(m) {
    if (migrateMsg) migrateMsg.textContent = m || '';
  }

  document.getElementById('fnmigrate').addEventListener('click', function () {
    var t =
      typeof __freenet_user_token !== 'undefined' ? __freenet_user_token : null;
    if (!t) {
      setOk('No key on this connection');
      return;
    }
    setMigrateMsg('Preparing your one-time migration link...');
    fetch('/v1/hosted/migrate/mint', {
      method: 'POST',
      headers: { 'X-Freenet-User-Token': t },
    })
      .then(function (r) {
        if (!r.ok) {
          throw new Error('HTTP ' + r.status);
        }
        return r.json();
      })
      .then(function (data) {
        var pt = data && data.pull_token;
        if (!pt) {
          throw new Error('no token');
        }
        var link =
          'http://127.0.0.1:' +
          LOCAL_PEER_PORT +
          '/hosted/import?source=' +
          encodeURIComponent(window.location.origin) +
          '&pt=' +
          encodeURIComponent(pt);
        if (migrateLink) migrateLink.value = link;
        if (migrateOut) migrateOut.hidden = false;
        setMigrateMsg('One-time link ready. It expires in a few minutes.');
      })
      .catch(function (e) {
        setMigrateMsg('Could not prepare a link (' + e.message + ')');
      });
  });

  document
    .getElementById('fnmigratecopy')
    .addEventListener('click', function () {
      if (!migrateLink || !migrateLink.value) {
        return;
      }
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(migrateLink.value).then(
          function () {
            setMigrateMsg('Link copied to clipboard');
          },
          function () {
            migrateLink.focus();
            migrateLink.select();
          },
        );
      } else {
        migrateLink.focus();
        migrateLink.select();
      }
    });
})();
