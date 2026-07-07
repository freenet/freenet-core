(function () {
  var params = new URLSearchParams(window.location.search);
  var source = params.get('source') || '';
  var pt = params.get('pt') || '';

  var statusEl = document.getElementById('status');
  var btn = document.getElementById('import-btn');
  var sourceLine = document.getElementById('source-line');
  var sourceEl = document.getElementById('source');

  function setStatus(msg, kind) {
    statusEl.textContent = msg || '';
    statusEl.className = 'status' + (kind ? ' status-' + kind : '');
  }

  function isHttps(u) {
    return /^https:\/\//i.test(u);
  }

  // Show the source as TEXT (never HTML) so the user sees where the data comes
  // from before confirming, with no injection surface.
  if (source) {
    sourceEl.textContent = source;
    sourceLine.hidden = false;
  }

  // Basic client-side sanity check. The server re-validates the source against
  // a strict allowlist regardless, so this is only for a friendly early error.
  if (!source || !pt || !isHttps(source)) {
    setStatus(
      'This import link is missing or malformed. Open the link from your ' +
        'hosted session again.',
      'error',
    );
    if (btn) btn.disabled = true;
    return;
  }

  // The import runs ONLY on an explicit click — never automatically on page
  // load — so merely navigating to the link cannot import anything.
  btn.addEventListener('click', function () {
    btn.disabled = true;
    setStatus('Importing your data. This can take a moment...', '');
    fetch('/v1/hosted/pull-import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source: source, pt: pt }),
    })
      .then(function (r) {
        return r.text().then(function (body) {
          return { ok: r.ok, status: r.status, body: body };
        });
      })
      .then(function (res) {
        if (res.ok) {
          var imported = 0;
          var kept = 0;
          try {
            var data = JSON.parse(res.body);
            imported = data.imported || 0;
            kept = (data.skipped && data.skipped.length) || 0;
          } catch (e) {
            /* A 200 with an unparseable body still means success. */
          }
          var msg =
            'Imported ' + imported + (imported === 1 ? ' item.' : ' items.');
          if (kept) {
            msg += ' ' + kept + ' already existed here and were kept.';
          }
          setStatus(msg + ' You can now open Freenet.', 'ok');
          btn.textContent = 'Done';
        } else {
          var reason = (res.body || '').trim();
          setStatus(
            reason || 'Import failed (HTTP ' + res.status + ').',
            'error',
          );
          btn.disabled = false;
        }
      })
      .catch(function (e) {
        setStatus(
          'Import failed: ' + (e && e.message ? e.message : 'network error'),
          'error',
        );
        btn.disabled = false;
      });
  });
})();
