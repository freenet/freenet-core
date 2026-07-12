(function () {
  try {
    if (localStorage.getItem('theme') === 'light') {
      document.documentElement.setAttribute('data-theme', 'light');
    }
  } catch (e) {
    /* localStorage unavailable — default to dark */
  }
})();

function toggleTheme() {
  var isLight = document.documentElement.getAttribute('data-theme') === 'light';
  var icon = document.getElementById('theme-icon');
  if (isLight) {
    document.documentElement.removeAttribute('data-theme');
    if (icon)
      icon.textContent = '\u2600\uFE0F'; /* sun = click to switch to light */
    try {
      localStorage.removeItem('theme');
    } catch (e) {}
  } else {
    document.documentElement.setAttribute('data-theme', 'light');
    if (icon)
      icon.textContent = '\uD83C\uDF19'; /* moon = click to switch to dark */
    try {
      localStorage.setItem('theme', 'light');
    } catch (e) {}
  }
}

/* ── Toast notifications ── */
function showToast(msg, opts) {
  var container = document.getElementById('toast-container');
  if (!container) {
    container = document.createElement('div');
    container.id = 'toast-container';
    container.className = 'toast-container';
    document.body.appendChild(container);
  }
  var t = document.createElement('div');
  t.className = 'toast' + (opts && opts.error ? ' toast-error' : '');
  t.textContent = msg;
  container.appendChild(t);
  setTimeout(
    function () {
      t.style.transition = 'opacity 0.25s';
      t.style.opacity = '0';
      setTimeout(function () {
        if (t.parentNode) t.parentNode.removeChild(t);
      }, 260);
    },
    (opts && opts.duration) || 1600,
  );
}

/* ── Copy contract key to clipboard ── */
function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text);
  }
  /* Fallback for older browsers / non-secure contexts */
  return new Promise(function (resolve, reject) {
    try {
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      var ok = document.execCommand('copy');
      document.body.removeChild(ta);
      ok ? resolve() : reject(new Error('execCommand failed'));
    } catch (e) {
      reject(e);
    }
  });
}

/* ── Sortable tables ── */
function compareCells(a, b, type) {
  if (type === 'num') {
    var na = parseFloat(a);
    var nb = parseFloat(b);
    var aBad = isNaN(na),
      bBad = isNaN(nb);
    if (aBad && bBad) return 0;
    if (aBad) return 1; /* missing values sort to bottom */
    if (bBad) return -1;
    return na - nb;
  }
  return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
}

function applySort(table, colIndex, dir) {
  var tbody = table.querySelector('tbody');
  if (!tbody) return;
  var ths = table.querySelectorAll('thead th');
  var th = ths[colIndex];
  if (!th) return;
  var type = th.getAttribute('data-sort-type') || 'text';
  var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
  rows.sort(function (r1, r2) {
    var c1 = r1.children[colIndex];
    var c2 = r2.children[colIndex];
    var v1 = c1 ? c1.getAttribute('data-sort') || c1.textContent : '';
    var v2 = c2 ? c2.getAttribute('data-sort') || c2.textContent : '';
    var cmp = compareCells(v1, v2, type);
    return dir === 'desc' ? -cmp : cmp;
  });
  rows.forEach(function (r) {
    tbody.appendChild(r);
  });
  ths.forEach(function (h) {
    h.classList.remove('sort-asc', 'sort-desc');
  });
  th.classList.add(dir === 'desc' ? 'sort-desc' : 'sort-asc');
}

function sortKey(table) {
  return 'sort:' + (table.getAttribute('data-table-id') || 'tbl');
}

function handleHeaderClick(th) {
  var table = th.closest('table.sortable');
  if (!table) return;
  var ths = Array.prototype.slice.call(table.querySelectorAll('thead th'));
  var idx = ths.indexOf(th);
  if (idx < 0) return;
  var current = th.classList.contains('sort-asc')
    ? 'asc'
    : th.classList.contains('sort-desc')
      ? 'desc'
      : null;
  var dir = current === 'asc' ? 'desc' : 'asc';
  applySort(table, idx, dir);
  try {
    sessionStorage.setItem(sortKey(table), idx + ':' + dir);
  } catch (e) {}
}

function restoreSort() {
  document.querySelectorAll('table.sortable').forEach(function (table) {
    try {
      var saved = sessionStorage.getItem(sortKey(table));
      if (!saved) return;
      var parts = saved.split(':');
      var idx = parseInt(parts[0], 10);
      var dir = parts[1] === 'desc' ? 'desc' : 'asc';
      if (!isNaN(idx)) applySort(table, idx, dir);
    } catch (e) {}
  });
}

/* ── Update-available check (GitHub releases, cached 12h) ── */
function compareSemver(a, b) {
  var pa = String(a)
    .replace(/^v/, '')
    .split(/[.\-+]/);
  var pb = String(b)
    .replace(/^v/, '')
    .split(/[.\-+]/);
  var n = Math.max(pa.length, pb.length);
  for (var i = 0; i < n; i++) {
    var na = parseInt(pa[i], 10);
    var nb = parseInt(pb[i], 10);
    if (isNaN(na) && isNaN(nb)) {
      var s = (pa[i] || '').localeCompare(pb[i] || '');
      if (s !== 0) return s;
      continue;
    }
    if (isNaN(na)) return -1;
    if (isNaN(nb)) return 1;
    if (na !== nb) return na - nb;
  }
  return 0;
}

function showUpdateBadge(latestTag) {
  var el = document.getElementById('update-badge');
  if (!el) return;
  el.textContent = 'Update: v' + String(latestTag).replace(/^v/, '');
  el.title =
    'A newer Freenet release is available — click to view release notes';
  el.hidden = false;
}

function checkForUpdate() {
  var badge = document.getElementById('version-badge');
  if (!badge) return;
  var current = badge.getAttribute('data-version') || '';
  if (!current || current === '?') return;
  var TTL_MS = 12 * 60 * 60 * 1000;
  var now = Date.now();
  var cached = null;
  try {
    var raw = localStorage.getItem('freenet-update-check');
    if (raw) cached = JSON.parse(raw);
  } catch (e) {}
  if (
    cached &&
    cached.tag &&
    cached.checkedAt &&
    now - cached.checkedAt < TTL_MS
  ) {
    if (compareSemver(cached.tag, current) > 0) showUpdateBadge(cached.tag);
    return;
  }
  fetch('https://api.github.com/repos/freenet/freenet-core/releases/latest', {
    headers: { Accept: 'application/vnd.github+json' },
  })
    .then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    })
    .then(function (data) {
      var tag = data && data.tag_name;
      if (!tag) return;
      try {
        localStorage.setItem(
          'freenet-update-check',
          JSON.stringify({ tag: tag, checkedAt: now }),
        );
      } catch (e) {}
      if (compareSemver(tag, current) > 0) showUpdateBadge(tag);
    })
    .catch(function (e) {
      /* Network blocked / GitHub rate-limited — silently skip */
      console.debug('Update check failed:', e);
    });
}

/* A version string is "known" when it is non-empty and not the '?'
   placeholder the homepage uses before a node snapshot exists.
   Mirrors version_is_known() in home_page.rs — keep both in sync. */
function versionIsKnown(v) {
  return !!v && v !== '?';
}

/* Show the stale-assets banner iff both the asset version (baked into this
   served page at compile time) and the live runtime version are known and
   differ. Mirrors should_show_version_banner() in home_page.rs. The point
   of comparing against a LIVE fetch (not the rendered data-version) is to
   catch the #4289 case: the browser is holding a cached page emitted by an
   old binary while a newer binary is now answering requests. */
function checkVersionMismatch() {
  var banner = document.getElementById('version-mismatch-banner');
  if (!banner) return;
  var assetVersion = banner.getAttribute('data-asset-version') || '';
  if (!versionIsKnown(assetVersion)) return;
  fetch('/v1/version', { headers: { Accept: 'application/json' } })
    .then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    })
    .then(function (data) {
      var runtimeVersion = data && data.version;
      if (!versionIsKnown(runtimeVersion)) return;
      if (runtimeVersion !== assetVersion) {
        banner.textContent =
          'Asset version ' +
          assetVersion +
          ' ≠ node version ' +
          runtimeVersion +
          ' — this page is stale, refresh to load the current version.';
        banner.hidden = false;
      } else {
        /* Versions agree (e.g. after a refresh fixed the staleness). */
        banner.hidden = true;
      }
    })
    .catch(function (e) {
      /* Endpoint unreachable / node mid-startup — don't show a spurious banner. */
      console.debug('Version check failed:', e);
    });
}

/* Tab switching for per-operation-type charts */
function switchTab(el) {
  var tabId = el.getAttribute('data-tab');
  /* Deactivate all tabs and panels in this group */
  var group = el.closest('.tab-group');
  if (!group) return;
  group.querySelectorAll('.tab-label').forEach(function (t) {
    t.classList.remove('tab-active');
  });
  group.querySelectorAll('.tab-panel').forEach(function (p) {
    p.classList.remove('tab-panel-active');
  });
  /* Activate selected */
  el.classList.add('tab-active');
  var panel = group.querySelector('#panel-' + tabId);
  if (panel) panel.classList.add('tab-panel-active');
  /* Remember active tab for auto-refresh persistence */
  try {
    sessionStorage.setItem('activeOpTab', tabId);
  } catch (e) {}
}

/* ── Import data (.fnsx) modal ──
   The receiving end of #4592: a user who exported a `freenet-data.fnsx` bundle
   from a hosted "try Freenet" server uploads it here to import their delegate
   secrets into THIS local peer via `POST /v1/import`. The modal lives outside
   <main> (see home.html) so the 5s auto-refresh never wipes it. */
function setImportStatus(msg, isError) {
  var el = document.getElementById('import-status');
  if (!el) return;
  el.textContent = msg || '';
  el.classList.toggle('import-status-error', !!isError);
}

function updateImportKeyLabel() {
  var kind = document.getElementById('import-key-kind');
  var label = document.getElementById('import-key-label');
  var input = document.getElementById('import-key');
  var isPass = kind && kind.value === 'passphrase';
  if (label) label.textContent = isPass ? 'Passphrase' : 'Access key';
  if (input)
    input.placeholder = isPass
      ? 'Enter your passphrase'
      : 'Paste your access key';
}

function openImportModal() {
  var modal = document.getElementById('import-modal');
  if (!modal) return;
  setImportStatus('', false);
  updateImportKeyLabel();
  modal.hidden = false;
  var file = document.getElementById('import-file');
  if (file) file.focus();
}

function closeImportModal() {
  var modal = document.getElementById('import-modal');
  if (!modal) return;
  modal.hidden = true;
  /* Clear the secret key (and the rest of the form) from the DOM on close. */
  var key = document.getElementById('import-key');
  if (key) key.value = '';
  var file = document.getElementById('import-file');
  if (file) file.value = '';
  var overwrite = document.getElementById('import-overwrite');
  if (overwrite) overwrite.checked = false;
  setImportStatus('', false);
}

function runImport() {
  var fileInput = document.getElementById('import-file');
  var keyInput = document.getElementById('import-key');
  var kindSel = document.getElementById('import-key-kind');
  var overwrite = document.getElementById('import-overwrite');
  var submit = document.getElementById('import-submit');

  var file = fileInput && fileInput.files && fileInput.files[0];
  if (!file) {
    setImportStatus('Choose a .fnsx file to import.', true);
    return;
  }
  var key = keyInput ? keyInput.value.trim() : '';
  if (!key) {
    setImportStatus('Enter the key that protects the bundle.', true);
    return;
  }

  var headers = {
    'X-Freenet-Bundle-Key': key,
    'X-Freenet-Bundle-Key-Kind': kindSel ? kindSel.value : 'token',
  };
  if (overwrite && overwrite.checked) {
    headers['X-Freenet-Import-Overwrite'] = 'true';
  }

  if (submit) submit.disabled = true;
  setImportStatus('Importing…', false);

  /* POST the raw file bytes as the body (application/octet-stream, NOT
     multipart) — /v1/import reads the body verbatim. The browser attaches an
     Origin header to this same-origin POST, which the import gate requires
     (loopback + trusted dashboard origin, no per-contract token). */
  fetch('/v1/import', { method: 'POST', headers: headers, body: file })
    .then(function (r) {
      return r.text().then(function (body) {
        return { ok: r.ok, status: r.status, body: body };
      });
    })
    .then(function (res) {
      if (submit) submit.disabled = false;
      if (res.ok) {
        var imported = 0;
        var skipped = 0;
        try {
          var data = JSON.parse(res.body);
          imported = data.imported || 0;
          skipped = (data.skipped && data.skipped.length) || 0;
        } catch (e) {
          /* A 200 with an unparseable body still means success. */
        }
        var msg =
          'Imported ' + imported + (imported === 1 ? ' secret' : ' secrets');
        if (skipped) msg += ', ' + skipped + ' skipped (already present)';
        closeImportModal();
        showToast(msg);
      } else {
        /* Non-2xx bodies are plain, non-secret reason strings from the
           endpoint (wrong-key 422, forbidden 403, too-large 413, ...). */
        var reason = (res.body || '').trim();
        setImportStatus(
          reason || 'Import failed (HTTP ' + res.status + ')',
          true,
        );
      }
    })
    .catch(function (e) {
      if (submit) submit.disabled = false;
      setImportStatus(
        'Import failed: ' + (e && e.message ? e.message : 'network error'),
        true,
      );
    });
}

document.addEventListener('DOMContentLoaded', function () {
  var icon = document.getElementById('theme-icon');
  if (icon && document.documentElement.getAttribute('data-theme') === 'light') {
    icon.textContent = '\uD83C\uDF19'; /* moon = click to switch to dark */
  }

  /* Restore active tab after page load / auto-refresh */
  function restoreTab() {
    try {
      var saved = sessionStorage.getItem('activeOpTab');
      if (saved) {
        var tab = document.querySelector(
          '.tab-label[data-tab="' + saved + '"]',
        );
        if (tab) switchTab(tab);
      }
    } catch (e) {}
  }
  restoreTab();
  restoreSort();
  checkForUpdate();
  checkVersionMismatch();

  /* Import modal controls. These elements live OUTSIDE <main> (see home.html),
     so they are stable across auto-refresh and can be bound directly, once. */
  var importCancel = document.getElementById('import-cancel');
  if (importCancel) importCancel.addEventListener('click', closeImportModal);
  var importSubmit = document.getElementById('import-submit');
  if (importSubmit) importSubmit.addEventListener('click', runImport);
  var importKind = document.getElementById('import-key-kind');
  if (importKind) importKind.addEventListener('change', updateImportKeyLabel);
  var importOverlay = document.getElementById('import-modal');
  if (importOverlay) {
    importOverlay.addEventListener('click', function (ev) {
      /* A click on the backdrop (not the card) dismisses the modal. */
      if (ev.target === importOverlay) closeImportModal();
    });
  }
  var importKey = document.getElementById('import-key');
  if (importKey) {
    importKey.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        runImport();
      }
    });
  }
  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Escape') return;
    var modal = document.getElementById('import-modal');
    if (modal && !modal.hidden) closeImportModal();
  });

  /* Delegated click handler \u2014 survives <main> innerHTML swaps from auto-refresh,
       so we don't need to re-bind after each refresh. */
  document.addEventListener('click', function (ev) {
    /* The open button is inside <main>, which auto-refresh re-renders, so it
       must be handled via delegation to survive innerHTML swaps. */
    var importOpen = ev.target.closest && ev.target.closest('.import-open-btn');
    if (importOpen) {
      ev.preventDefault();
      openImportModal();
      return;
    }
    var copy = ev.target.closest && ev.target.closest('.copy-key');
    if (copy) {
      ev.preventDefault();
      ev.stopPropagation();
      var text = copy.getAttribute('data-copy') || copy.textContent.trim();
      copyToClipboard(text)
        .then(function () {
          showToast('Contract key copied');
          copy.classList.add('copied');
          setTimeout(function () {
            copy.classList.remove('copied');
          }, 900);
        })
        .catch(function () {
          showToast('Copy failed', { error: true });
        });
      return;
    }
    var th = ev.target.closest && ev.target.closest('table.sortable thead th');
    if (th) {
      handleHeaderClick(th);
      return;
    }
  });

  /* Auto-refresh: fetch the page and swap dynamic content without a full reload.
       Uses setTimeout chaining (not setInterval) so slow responses don't overlap.

       Refresh cadence follows tab visibility (#3353): a hidden/backgrounded tab
       backs off to a much longer interval since nobody is watching, and polling
       every 5s while backgrounded only burns CPU/battery and spams the local
       node with requests nobody reads. The moment the tab becomes visible again
       we refresh immediately (rather than waiting out the stale timer) so the
       user sees current data right away, then resume the fast cadence. */
  var VISIBLE_REFRESH_MS = 5000;
  var HIDDEN_REFRESH_MS = 60000;
  var refreshTimer = null;
  /* Guards against a second concurrent refreshDashboard() chain: without
     this, a visibilitychange->visible event racing against an in-flight
     timer-triggered fetch would clearTimeout() an id that already fired
     (a no-op) and then kick off a second .finally(scheduleRefresh) chain,
     permanently forking the "one fetch at a time" poll loop. */
  var refreshInFlight = false;

  function currentRefreshInterval() {
    return document.hidden ? HIDDEN_REFRESH_MS : VISIBLE_REFRESH_MS;
  }

  function refreshDashboard() {
    if (refreshInFlight) {
      /* A fetch is already running (either the timer-driven one or one
         kicked off by a prior visibilitychange) — do nothing so we never
         run two overlapping refresh chains. */
      return Promise.resolve();
    }
    refreshInFlight = true;
    return fetch(window.location.href)
      .then(function (r) {
        return r.text();
      })
      .then(function (html) {
        var parser = new DOMParser();
        var doc = parser.parseFromString(html, 'text/html');
        var newMain = doc.querySelector('main');
        var oldMain = document.querySelector('main');
        if (newMain && oldMain) oldMain.innerHTML = newMain.innerHTML;
        /* Update header elements (outside <main>) */
        var newUp = doc.querySelector('.uptime');
        var oldUp = document.querySelector('.uptime');
        if (newUp && oldUp) oldUp.textContent = newUp.textContent;
        var newBadge = doc.querySelector('#version-badge');
        var oldBadge = document.getElementById('version-badge');
        if (newBadge && oldBadge) {
          oldBadge.textContent = newBadge.textContent;
          var nv = newBadge.getAttribute('data-version');
          if (nv) oldBadge.setAttribute('data-version', nv);
        }
        var newIcon = doc.querySelector('link[rel="icon"]');
        var oldIcon = document.querySelector('link[rel="icon"]');
        if (newIcon && oldIcon)
          oldIcon.setAttribute('href', newIcon.getAttribute('href'));
        /* Restore tab selection and table sort after content swap */
        restoreTab();
        restoreSort();
        /* Re-check the live runtime version so the stale-assets banner
                 appears (or clears) if the serving process changes while the
                 page stays open. The banner's data-asset-version stays anchored
                 to the originally-loaded page, which is the version we're
                 comparing against. */
        checkVersionMismatch();
      })
      .catch(function (e) {
        console.warn('Dashboard refresh failed:', e);
      })
      .finally(function () {
        refreshInFlight = false;
      });
  }

  function scheduleRefresh() {
    if (refreshTimer !== null) clearTimeout(refreshTimer);
    refreshTimer = setTimeout(function () {
      /* Clear before firing: once this callback runs, the timer id is
         already spent, so leaving refreshTimer set to it would make a
         concurrent clearTimeout(refreshTimer) elsewhere a silent no-op. */
      refreshTimer = null;
      refreshDashboard().finally(scheduleRefresh);
    }, currentRefreshInterval());
  }

  /* When the tab regains visibility, refresh right away instead of waiting
     out whatever remains of the hidden-tab backoff timer, then fall back to
     the normal cadence for the next tick. If a refresh is already in flight
     (e.g. the hidden-tab timer fired just before visibility changed), just
     reschedule at the normal cadence instead of starting a second fetch. */
  document.addEventListener('visibilitychange', function () {
    if (document.hidden) return;
    if (refreshTimer !== null) clearTimeout(refreshTimer);
    refreshTimer = null;
    if (refreshInFlight) {
      scheduleRefresh();
      return;
    }
    refreshDashboard().finally(scheduleRefresh);
  });

  scheduleRefresh();
});
