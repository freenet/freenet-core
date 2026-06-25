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
})();
