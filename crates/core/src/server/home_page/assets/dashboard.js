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

/* Update-check core, extracted so the caching/back-off state machine can be
   driven under Node by update_check.test.mjs. All browser dependencies are
   injected, so this stays free of direct window/document/fetch references —
   keep it that way, and keep it bracketed by the markers.

   Invariants (tested behaviorally in update_check.test.mjs):
   - a fresh SUCCESS within TTL_MS is served from cache with no request;
   - a FAILURE is remembered and suppresses further requests for FAIL_TTL_MS
     (#5102: previously only success was cached, so a rate-limited browser
     re-requested on every single page load — knocking hardest exactly while
     the IP was already being refused);
   - the failure window expires, so a limited client recovers on its own;
   - a 200 carrying no usable tag counts as a failure, so a malformed
     response cannot drive a per-reload retry loop;
   - the badge is shown only when the fetched tag is strictly newer. */
/* update-check:BEGIN */
function createUpdateChecker(deps) {
  var TTL_MS = 12 * 60 * 60 * 1000;
  var FAIL_TTL_MS = 60 * 60 * 1000;

  function readCache() {
    try {
      var raw = deps.getItem('freenet-update-check');
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      return null;
    }
  }

  function write(entry) {
    try {
      deps.setItem('freenet-update-check', JSON.stringify(entry));
    } catch (e) {
      /* storage unavailable (private mode / quota) — degrade to no caching */
    }
  }

  /* Returns a promise so callers/tests can await settling; the production
     caller ignores it. */
  function check(current) {
    if (!current || current === '?') return Promise.resolve('skipped');
    var now = deps.now();
    var cached = readCache();

    if (
      cached &&
      cached.tag &&
      cached.checkedAt &&
      now - cached.checkedAt < TTL_MS
    ) {
      if (deps.compareSemver(cached.tag, current) > 0)
        deps.showBadge(cached.tag);
      return Promise.resolve('cached');
    }
    /* Back off after a failed check. GitHub's core limit resets hourly, so one
       retry per hour per browser is both polite and enough to recover without
       the user doing anything. */
    if (cached && cached.failedAt && now - cached.failedAt < FAIL_TTL_MS) {
      return Promise.resolve('backoff');
    }

    var rememberFailure = function () {
      write({ failedAt: now });
      return 'failed';
    };

    return deps
      .fetchLatest()
      .then(function (data) {
        var tag = data && data.tag_name;
        if (!tag) return rememberFailure();
        write({ tag: tag, checkedAt: now });
        if (deps.compareSemver(tag, current) > 0) deps.showBadge(tag);
        return 'fetched';
      })
      .catch(function (e) {
        /* Network blocked / GitHub rate-limited (403/429) — go quiet for
           FAIL_TTL_MS rather than retrying on every page load. Keep the debug
           line: it is the only client-side signal for a silently missing badge,
           and its sibling checkVersionMismatch() still logs one. */
        deps.onError(e);
        return rememberFailure();
      });
  }

  return { check: check };
}
/* update-check:END */

function checkForUpdate() {
  var badge = document.getElementById('version-badge');
  if (!badge) return;
  /* This check must stay on api.github.com — unlike the node's own poll it
     runs in a browser, and the quota-free github.com redirect sends no CORS
     headers so fetch() cannot read it. It therefore spends from the same
     60/hr per-IP REST budget, shared with every other machine behind the same
     NAT/CGNAT or VPN exit — hence the failure back-off above. */
  var checker = createUpdateChecker({
    now: function () {
      return Date.now();
    },
    getItem: function (k) {
      return localStorage.getItem(k);
    },
    setItem: function (k, v) {
      localStorage.setItem(k, v);
    },
    compareSemver: compareSemver,
    showBadge: showUpdateBadge,
    onError: function (e) {
      console.debug('Update check failed:', e);
    },
    fetchLatest: function () {
      return fetch(
        'https://api.github.com/repos/freenet/freenet-core/releases/latest',
        { headers: { Accept: 'application/vnd.github+json' } },
      ).then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      });
    },
  });
  checker.check(badge.getAttribute('data-version') || '');
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

/* ── Browser-local bandwidth chart (extracted for Node unit tests) ──
   The server exposes cumulative byte counters but does not mutate history when
   a page is read. This controller keeps one bounded history per browser page,
   so other tabs, peer-detail requests, and crawlers cannot change the graph.
   All clock/DOM dependencies are injected for deterministic Node tests. */
/* bandwidth-chart-factory:BEGIN */
function createBandwidthChart(deps) {
  var WINDOW_MS = 60 * 1000;
  var MIN_SAMPLE_MS = 1000;
  /* At most one accepted sample per second plus the inclusive window edge. */
  var MAX_SAMPLES = 61;
  var SVG_NS = 'http://www.w3.org/2000/svg';
  var baseline = null;
  var samples = [];
  var lastUptime = null;
  var lastObservedAt = null;
  var tooltip = null;
  var lastSvg = null;
  var restoreDataOpen = false;
  var restoreDataFocus = false;

  function asCounter(value) {
    if (
      value === null ||
      value === undefined ||
      (typeof value === 'string' && value.trim() === '')
    ) {
      return null;
    }
    try {
      var parsed = typeof value === 'bigint' ? value : BigInt(value);
      return parsed >= 0n ? parsed : null;
    } catch (e) {
      return null;
    }
  }

  function clearHistory() {
    baseline = null;
    samples = [];
  }

  function resetSamples(up, down, at) {
    baseline = { up: up, down: down, at: at };
    samples = [];
  }

  function observeTotals(upTotal, downTotal, now) {
    var up = asCounter(upTotal);
    var down = asCounter(downTotal);
    if (up === null || down === null || !Number.isFinite(now)) {
      clearHistory();
      return false;
    }

    if (baseline === null) {
      resetSamples(up, down, now);
      return false;
    }

    var elapsedMs = now - baseline.at;
    if (elapsedMs < 0) {
      resetSamples(up, down, now);
      return false;
    }
    if (elapsedMs === 0) return false;

    /* A process restart resets cumulative counters. Start a fresh series
       instead of turning the reset into a giant or negative rate. */
    if (up < baseline.up || down < baseline.down) {
      resetSamples(up, down, now);
      return false;
    }

    /* A point represents only its preceding interval. After a suspended or
       closed page, traffic from beyond the visible minute must not be stamped
       as current, so discard both the old series and the stale interval. */
    if (elapsedMs > WINDOW_MS) {
      resetSamples(up, down, now);
      return false;
    }

    /* Coalesce bursts of refreshes. Do not advance the baseline here: the next
       accepted sample must include these bytes and the full elapsed interval. */
    if (elapsedMs < MIN_SAMPLE_MS) return false;

    var upBps = (Number(up - baseline.up) * 1000) / elapsedMs;
    var downBps = (Number(down - baseline.down) * 1000) / elapsedMs;
    if (!Number.isFinite(upBps) || !Number.isFinite(downBps)) {
      resetSamples(up, down, now);
      return false;
    }

    baseline = { up: up, down: down, at: now };
    samples.push({ at: now, upBps: upBps, downBps: downBps });

    var cutoff = now - WINDOW_MS;
    while (samples.length && samples[0].at < cutoff) samples.shift();
    while (samples.length > MAX_SAMPLES) samples.shift();
    return true;
  }

  function observeSnapshot(up, down, uptimeTotal, now) {
    var uptime = asCounter(uptimeTotal);
    if (uptime === null || !Number.isFinite(now)) {
      clearHistory();
      lastUptime = null;
      lastObservedAt = null;
      return false;
    }

    if (lastUptime !== null && lastObservedAt !== null) {
      var elapsedSeconds = Math.max(
        0,
        Math.floor((now - lastObservedAt) / 1000),
      );
      var elapsedWhole = BigInt(elapsedSeconds);
      var uptimeAdvance = uptime >= lastUptime ? uptime - lastUptime : 0n;
      var uptimeTooSlow =
        elapsedSeconds > 1 && uptimeAdvance + 1n < elapsedWhole;
      var uptimeTooFast = uptimeAdvance > elapsedWhole + 1n;
      /* A node restart or a browser clock paused during system sleep makes
         node uptime diverge from the local monotonic interval. Neither case
         may bridge byte counters into a misleading current rate. */
      if (uptime < lastUptime || uptimeTooSlow || uptimeTooFast) clearHistory();
    }

    lastUptime = uptime;
    lastObservedAt = now;
    return observeTotals(up, down, now);
  }

  function formatRate(value) {
    var n = Math.max(0, Number(value) || 0);
    var units = ['B/s', 'KB/s', 'MB/s', 'GB/s', 'TB/s'];
    var unit = 0;
    while (n >= 1024 && unit < units.length - 1) {
      n /= 1024;
      unit += 1;
    }
    var number;
    if (unit === 0 && Number.isInteger(n)) number = String(n);
    else number = n.toFixed(1);
    return number + ' ' + units[unit];
  }

  function formatAge(ageMs) {
    var seconds = Math.max(0, Math.round(ageMs / 1000));
    if (seconds === 0) return 'now';
    if (seconds < 60) return '-' + seconds + 's';
    if (seconds < 3600) return '-' + Math.floor(seconds / 60) + 'm';
    return '-' + Math.floor(seconds / 3600) + 'h';
  }

  function renderMarkup(history, dataOpen) {
    if (history.length < 2) {
      return '<p class="empty bw-collecting">Collecting bandwidth samples…</p>';
    }

    var width = 560;
    var height = 150;
    var padRight = 12;
    var padTop = 12;
    var padBottom = 24;
    var plotHeight = height - padTop - padBottom;
    var newestAt = history[history.length - 1].at;
    var windowStart = newestAt - WINDOW_MS;
    var yMax = 1;
    for (var i = 0; i < history.length; i++) {
      yMax = Math.max(yMax, history[i].upBps, history[i].downBps);
    }

    var yValues = [0, yMax / 2, yMax];
    var yLabels = yValues.map(formatRate);
    var longestLabel = Math.max.apply(
      null,
      yLabels.map(function (label) {
        return label.length;
      }),
    );
    var padLeft = Math.max(70, longestLabel * 6 + 10);
    var plotWidth = width - padLeft - padRight;
    var baselineY = padTop + plotHeight;
    var plotRight = padLeft + plotWidth;

    function toX(sample) {
      return padLeft + ((sample.at - windowStart) / WINDOW_MS) * plotWidth;
    }

    function toY(value) {
      return padTop + plotHeight - (value / yMax) * plotHeight;
    }

    var html =
      '<svg viewBox="0 0 ' +
      width +
      ' ' +
      height +
      '" class="chart-svg bw-chart" role="img" aria-labelledby="bandwidth-chart-title bandwidth-chart-desc" data-plot-top="' +
      padTop +
      '" data-plot-bottom="' +
      baselineY +
      '">' +
      '<title id="bandwidth-chart-title">Recent Freenet payload transfer rates</title>' +
      '<desc id="bandwidth-chart-desc">Upload is a solid line and download is a dashed line, measured in bytes per second over the last minute.</desc>' +
      '<line x1="' +
      padLeft +
      '" y1="' +
      padTop +
      '" x2="' +
      padLeft +
      '" y2="' +
      baselineY +
      '" class="bw-axis"/>' +
      '<line x1="' +
      padLeft +
      '" y1="' +
      baselineY +
      '" x2="' +
      plotRight +
      '" y2="' +
      baselineY +
      '" class="bw-axis"/>';

    for (i = 0; i < yValues.length; i++) {
      var y = toY(yValues[i]);
      html +=
        '<line x1="' +
        padLeft +
        '" y1="' +
        y.toFixed(1) +
        '" x2="' +
        plotRight +
        '" y2="' +
        y.toFixed(1) +
        '" class="bw-grid"/>' +
        '<text x="' +
        (padLeft - 4) +
        '" y="' +
        (y + 4).toFixed(1) +
        '" text-anchor="end" class="axis-label">' +
        yLabels[i] +
        '</text>';
    }

    var tickCount = 5;
    for (i = 0; i < tickCount; i++) {
      var fraction = i / (tickCount - 1);
      var anchor = i === 0 ? 'start' : i === tickCount - 1 ? 'end' : 'middle';
      html +=
        '<text x="' +
        (padLeft + fraction * plotWidth).toFixed(1) +
        '" y="' +
        (baselineY + 16) +
        '" text-anchor="' +
        anchor +
        '" class="axis-label">' +
        formatAge(WINDOW_MS * (1 - fraction)) +
        '</text>';
    }

    function points(field) {
      return history
        .map(function (sample) {
          return toX(sample).toFixed(1) + ',' + toY(sample[field]).toFixed(1);
        })
        .join(' ');
    }

    html +=
      '<polyline class="bw-series bw-series-up" points="' +
      points('upBps') +
      '"/>' +
      '<polyline class="bw-series bw-series-down" points="' +
      points('downBps') +
      '"/>';

    for (i = 0; i < history.length; i++) {
      var sample = history[i];
      html +=
        '<circle class="bw-hit" cx="' +
        toX(sample).toFixed(1) +
        '" cy="' +
        baselineY +
        '" r="8" data-up-y="' +
        toY(sample.upBps).toFixed(1) +
        '" data-down-y="' +
        toY(sample.downBps).toFixed(1) +
        '" data-age-label="' +
        formatAge(newestAt - sample.at) +
        '" data-up-label="' +
        formatRate(sample.upBps) +
        '" data-down-label="' +
        formatRate(sample.downBps) +
        '" aria-hidden="true"/>';
    }

    html +=
      '</svg>' +
      '<div class="bw-legend" aria-hidden="true">' +
      '<span class="bw-key"><span class="bw-line bw-line-up"></span> Upload</span>' +
      '<span class="bw-key"><span class="bw-line bw-line-down"></span> Download</span>' +
      '</div>' +
      '<details class="bw-data"' +
      (dataOpen ? ' open' : '') +
      '><summary class="bw-data-summary">View sample data</summary>' +
      '<table><thead><tr><th>Time</th><th>Upload</th><th>Download</th></tr></thead><tbody>';
    for (i = 0; i < history.length; i++) {
      sample = history[i];
      html +=
        '<tr><td>' +
        formatAge(newestAt - sample.at) +
        '</td><td>' +
        formatRate(sample.upBps) +
        '</td><td>' +
        formatRate(sample.downBps) +
        '</td></tr>';
    }
    return html + '</tbody></table></details>';
  }

  function clearOverlay() {
    if (lastSvg) {
      var overlay = lastSvg.querySelector('.bw-overlay');
      if (overlay && overlay.parentNode === lastSvg)
        lastSvg.removeChild(overlay);
      lastSvg = null;
    }
    if (tooltip) tooltip.style.display = 'none';
  }

  function render(host) {
    clearOverlay();
    var content = host && host.querySelector('.bw-chart-content');
    if (!content) {
      restoreDataOpen = false;
      restoreDataFocus = false;
      return false;
    }
    content.innerHTML = renderMarkup(samples, restoreDataOpen);
    if (restoreDataFocus) {
      var summary = content.querySelector('.bw-data-summary');
      if (summary) summary.focus();
    }
    restoreDataOpen = false;
    restoreDataFocus = false;
    return true;
  }

  function beforeRefresh(root) {
    clearOverlay();
    var data = root && root.querySelector('.bw-data');
    restoreDataOpen = Boolean(data && data.open);
    var active = deps.activeElement();
    restoreDataFocus = Boolean(data && active && data.contains(active));
  }

  function sampleAndRender(root) {
    var host = root && root.querySelector('[data-bandwidth-chart]');
    if (!host) {
      clearOverlay();
      clearHistory();
      lastUptime = null;
      lastObservedAt = null;
      restoreDataOpen = false;
      restoreDataFocus = false;
      return false;
    }
    observeSnapshot(
      host.getAttribute('data-bytes-uploaded'),
      host.getAttribute('data-bytes-downloaded'),
      host.getAttribute('data-node-uptime-secs'),
      deps.now(),
    );
    return render(host);
  }

  function viewBoxX(rect, viewBox, clientX) {
    var parts = viewBox.trim().split(/\s+/);
    var vbX = parseFloat(parts[0]) || 0;
    var vbWidth = parseFloat(parts[2]) || 0;
    if (!rect.width || !vbWidth) return vbX;
    return vbX + ((clientX - rect.left) / rect.width) * vbWidth;
  }

  function nearestHit(hits, x) {
    var best = null;
    var bestDistance = Infinity;
    for (var i = 0; i < hits.length; i++) {
      var distance = Math.abs(parseFloat(hits[i].getAttribute('cx')) - x);
      if (distance < bestDistance) {
        bestDistance = distance;
        best = hits[i];
      }
    }
    return best;
  }

  function overlayHtml(hit, plotTop, plotBottom) {
    var x = parseFloat(hit.getAttribute('cx'));
    return (
      '<line class="bw-guide" x1="' +
      x +
      '" y1="' +
      plotTop +
      '" x2="' +
      x +
      '" y2="' +
      plotBottom +
      '"/>' +
      '<circle class="bw-dot bw-dot-up" cx="' +
      x +
      '" cy="' +
      hit.getAttribute('data-up-y') +
      '" r="3.5"/>' +
      '<circle class="bw-dot bw-dot-down" cx="' +
      x +
      '" cy="' +
      hit.getAttribute('data-down-y') +
      '" r="3.5"/>'
    );
  }

  function tooltipHtml(hit) {
    return (
      '<div class="bw-tooltip-time">' +
      hit.getAttribute('data-age-label') +
      '</div>' +
      '<div class="bw-tooltip-row"><span class="bw-line bw-line-up"></span><span>Up ' +
      hit.getAttribute('data-up-label') +
      '</span></div>' +
      '<div class="bw-tooltip-row"><span class="bw-line bw-line-down"></span><span>Down ' +
      hit.getAttribute('data-down-label') +
      '</span></div>'
    );
  }

  function positionTooltip(element, clientX, clientY) {
    var viewportWidth = deps.innerWidth();
    var viewportHeight = deps.innerHeight();
    var tooltipWidth = Math.min(190, Math.max(0, viewportWidth - 8));
    var tooltipHeight = 68;
    var left = clientX + 14;
    var top = clientY + 14;
    if (left + tooltipWidth > viewportWidth - 4)
      left = clientX - tooltipWidth - 8;
    if (top + tooltipHeight > viewportHeight - 4)
      top = clientY - tooltipHeight - 8;
    left = Math.max(4, Math.min(left, viewportWidth - tooltipWidth - 4));
    top = Math.max(4, Math.min(top, viewportHeight - tooltipHeight - 4));
    element.style.left = left + 'px';
    element.style.top = top + 'px';
    element.style.maxWidth = tooltipWidth + 'px';
  }

  function ensureTooltip() {
    if (tooltip) return tooltip;
    tooltip = deps.createEl('div');
    tooltip.setAttribute('class', 'bw-tooltip');
    tooltip.style.display = 'none';
    deps.appendToBody(tooltip);
    return tooltip;
  }

  function renderOverlay(svg, hit) {
    var plotTop = parseFloat(svg.getAttribute('data-plot-top'));
    var plotBottom = parseFloat(svg.getAttribute('data-plot-bottom'));
    var overlay = svg.querySelector('.bw-overlay');
    if (!overlay) {
      overlay = deps.createElNS(SVG_NS, 'g');
      overlay.setAttribute('class', 'bw-overlay');
      svg.appendChild(overlay);
    }
    overlay.innerHTML = overlayHtml(hit, plotTop, plotBottom);
  }

  function showHit(svg, hit, clientX, clientY) {
    if (!hit) {
      clearOverlay();
      return;
    }
    if (lastSvg && lastSvg !== svg) clearOverlay();
    lastSvg = svg;
    renderOverlay(svg, hit);
    var tip = ensureTooltip();
    tip.innerHTML = tooltipHtml(hit);
    tip.style.display = 'block';
    positionTooltip(tip, clientX, clientY);
  }

  function onPointer(ev) {
    var target = ev.target;
    var svg = target && target.closest ? target.closest('svg.bw-chart') : null;
    if (!svg) {
      clearOverlay();
      return;
    }
    var hits = svg.querySelectorAll('.bw-hit');
    if (!hits.length) {
      clearOverlay();
      return;
    }
    var x = viewBoxX(
      deps.getRect(svg),
      svg.getAttribute('viewBox'),
      ev.clientX,
    );
    showHit(svg, nearestHit(hits, x), ev.clientX, ev.clientY);
  }

  return {
    observeTotals: observeTotals,
    renderMarkup: renderMarkup,
    sampleAndRender: sampleAndRender,
    beforeRefresh: beforeRefresh,
    onPointer: onPointer,
    clear: clearOverlay,
    viewBoxX: viewBoxX,
    nearestHit: nearestHit,
    positionTooltip: positionTooltip,
    getSamples: function () {
      return samples.slice();
    },
  };
}
/* bandwidth-chart-factory:END */

/* ── Auto-refresh scheduler (extracted for Node unit tests) ──
   The scheduling state machine is deliberately self-contained: every browser
   dependency (timers, the actual fetch/DOM-swap refresh, visibility state) is
   injected via `deps`, so dashboard_refresh.test.mjs can extract this factory
   verbatim (between the refresh-scheduler:BEGIN/END markers) and drive it
   under Node with fake timers. Keep it free of direct references to
   window/document, and keep it bracketed by those markers.

   Invariants (tested behaviorally in dashboard_refresh.test.mjs):
   - one fetch at a time: refreshDashboard() is a no-op while a refresh is
     in flight (refreshInFlight guard);
   - the poll chain never forks: refreshTimer is reset to null the moment its
     setTimeout callback fires, so a concurrent clearTimeout(refreshTimer)
     can never silently no-op against an already-spent timer id;
   - hidden tabs poll at HIDDEN_REFRESH_MS, visible tabs at
     VISIBLE_REFRESH_MS (#3353), and becoming visible refreshes immediately
     unless a refresh is already running (then it just reschedules). */
/* refresh-scheduler:BEGIN */
function createRefreshScheduler(deps) {
  var VISIBLE_REFRESH_MS = 5000;
  var HIDDEN_REFRESH_MS = 60000;
  var refreshTimer = null;
  /* Guards against a second concurrent refreshDashboard() chain: without
     this, a visibilitychange->visible event racing against an in-flight
     timer-triggered fetch would clearTimeout() an id that already fired
     (a no-op) and then kick off a second .finally(scheduleRefresh) chain,
     breaking the "one fetch at a time" invariant. */
  var refreshInFlight = false;

  function currentRefreshInterval() {
    return deps.isHidden() ? HIDDEN_REFRESH_MS : VISIBLE_REFRESH_MS;
  }

  function refreshDashboard() {
    if (refreshInFlight) {
      /* A fetch is already running (either the timer-driven one or one
         kicked off by a prior visibilitychange) — do nothing so we never
         run two overlapping refresh chains. */
      return Promise.resolve();
    }
    refreshInFlight = true;
    return deps.refresh().finally(function () {
      refreshInFlight = false;
    });
  }

  function scheduleRefresh() {
    if (refreshTimer !== null) deps.clearTimeout(refreshTimer);
    refreshTimer = deps.setTimeout(function () {
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
  function onVisibilityChange() {
    if (deps.isHidden()) return;
    if (refreshTimer !== null) deps.clearTimeout(refreshTimer);
    refreshTimer = null;
    if (refreshInFlight) {
      scheduleRefresh();
      return;
    }
    refreshDashboard().finally(scheduleRefresh);
  }

  return {
    scheduleRefresh: scheduleRefresh,
    onVisibilityChange: onVisibilityChange,
  };
}
/* refresh-scheduler:END */

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
       user sees current data right away, then resume the fast cadence.

       Scheduling policy (intervals, in-flight dedup, timer bookkeeping) lives
       in createRefreshScheduler above; this block supplies the browser-real
       dependencies: the fetch + DOM-swap refresh, real timers, and
       document.hidden. */
  function fetchAndSwapDashboard() {
    return fetch(window.location.href)
      .then(function (r) {
        return r.text();
      })
      .then(function (html) {
        var parser = new DOMParser();
        var doc = parser.parseFromString(html, 'text/html');
        var newMain = doc.querySelector('main');
        var oldMain = document.querySelector('main');
        if (newMain && oldMain) {
          /* bandwidth-chart-refresh:BEGIN */
          bandwidthChart.beforeRefresh(document);
          oldMain.innerHTML = newMain.innerHTML;
          bandwidthChart.sampleAndRender(document);
          /* bandwidth-chart-refresh:END */
        }
        /* Update the tab title (connection state + count, #3509) so a
           backgrounded tab still surfaces the current status at a glance. */
        if (doc.title) document.title = doc.title;
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
      });
  }

  var refreshScheduler = createRefreshScheduler({
    setTimeout: function (fn, ms) {
      return setTimeout(fn, ms);
    },
    clearTimeout: function (id) {
      clearTimeout(id);
    },
    refresh: fetchAndSwapDashboard,
    isHidden: function () {
      return document.hidden;
    },
  });

  /* bandwidth-chart-wiring:BEGIN */
  var bandwidthChart = createBandwidthChart({
    now: function () {
      return window.performance.now();
    },
    getRect: function (el) {
      return el.getBoundingClientRect();
    },
    createEl: function (tag) {
      return document.createElement(tag);
    },
    /* SVG overlay elements need the SVG namespace or the browser won't
       paint them (see renderOverlay). */
    createElNS: function (ns, tag) {
      return document.createElementNS(ns, tag);
    },
    appendToBody: function (el) {
      document.body.appendChild(el);
    },
    innerWidth: function () {
      return window.innerWidth;
    },
    innerHeight: function () {
      return window.innerHeight;
    },
    activeElement: function () {
      return document.activeElement;
    },
  });
  bandwidthChart.sampleAndRender(document);
  document.addEventListener('pointermove', function (ev) {
    bandwidthChart.onPointer(ev);
  });
  document.addEventListener('pointerdown', function (ev) {
    bandwidthChart.onPointer(ev);
  });
  document.addEventListener('mouseleave', function () {
    bandwidthChart.clear();
  });
  /* bandwidth-chart-wiring:END */

  document.addEventListener(
    'visibilitychange',
    refreshScheduler.onVisibilityChange,
  );

  refreshScheduler.scheduleRefresh();
});
