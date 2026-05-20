#!/usr/bin/env bash
# Run Rust static-analysis checks and aggregate per-tool output into a single
# machine-readable JSON report. Each tool's raw output is also kept on disk so
# downstream consumers can dig further.
#
# Usage:
#   scripts/static-checks.sh [workspace-path]
#
# Default workspace-path is the script's own repository root. Pass an explicit
# path to run against a different cargo workspace.
#
# Output layout:
#   <workspace>/target/static-checks/<UTC-timestamp>/
#     report.json            — aggregate report (machine-readable)
#     <tool>.raw             — stdout of each tool
#     <tool>.err             — stderr of each tool
#     <tool>.summary.json    — per-tool status/duration/extracted summary
#
# Exit code is always 0 unless prerequisites are missing; individual tools may
# fail without aborting the run (each tool's exit code is recorded in JSON).

set -uo pipefail

REPO_ROOT_DEFAULT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${1:-$REPO_ROOT_DEFAULT}"
TARGET="$(cd "$TARGET" && pwd)"

if [[ ! -f "$TARGET/Cargo.toml" ]]; then
  echo "error: $TARGET has no Cargo.toml" >&2
  exit 2
fi

command -v jq    >/dev/null 2>&1 || { echo "error: jq is required"    >&2; exit 2; }
command -v cargo >/dev/null 2>&1 || { echo "error: cargo is required" >&2; exit 2; }

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$TARGET/target/static-checks/$TS"
mkdir -p "$OUT"

log() { printf '[static-checks] %s\n' "$*" >&2; }

# run_check NAME PROBE_BIN -- cmd args...
#
# PROBE_BIN is what we look up via `command -v` to detect installation.
# For cargo subcommands the probe is `cargo-<sub>` (the binary cargo dispatches
# to); for first-party cargo features the probe is `cargo` itself.
run_check() {
  local name="$1" probe="$2"; shift 2

  if ! command -v "$probe" >/dev/null 2>&1; then
    jq -n --arg p "$probe" '{status:"not_installed", probe:$p}' \
      > "$OUT/${name}.summary.json"
    log "skip   $name  (missing: $probe — try: cargo install ${probe#cargo-})"
    return
  fi

  log "run    $name"
  local t0; t0=$(date +%s)
  local rc=0
  "$@" > "$OUT/${name}.raw" 2> "$OUT/${name}.err" || rc=$?
  local t1; t1=$(date +%s)

  local status; [[ $rc -eq 0 ]] && status=ok || status="exit_${rc}"

  jq -n \
    --arg status  "$status" \
    --argjson rc  "$rc" \
    --argjson dur "$((t1-t0))" \
    --arg raw     "$OUT/${name}.raw" \
    --arg err     "$OUT/${name}.err" \
    '{status:$status, exit_code:$rc, duration_seconds:$dur, raw:$raw, err:$err}' \
    > "$OUT/${name}.summary.json"
}

cd "$TARGET"

# Disable colour for every child process so .raw captures stay plain text.
export NO_COLOR=1
export CARGO_TERM_COLOR=never

# Detect whether $TARGET/Cargo.toml is a virtual workspace manifest (workspace
# without a [package] section). Some tools (cargo-geiger, cargo-bloat) refuse
# to run in this mode and need per-member invocation.
is_virtual_manifest() {
  grep -q '^\[workspace\]' "$TARGET/Cargo.toml" 2>/dev/null \
    && ! grep -q '^\[package\]' "$TARGET/Cargo.toml" 2>/dev/null
}

# Emits "package_name<TAB>manifest_path" lines for every workspace member.
list_workspace_members() {
  cargo metadata --no-deps --format-version 1 2>/dev/null \
    | jq -r '.packages[] | "\(.name)\t\(.manifest_path)"'
}

# --- 1. clippy with perf + nursery lints, JSON diagnostics ----------------
run_check clippy cargo \
  cargo clippy --workspace --all-targets --message-format=json \
    -- -W clippy::perf -W clippy::nursery

# --- 2. cargo-audit — RustSec advisory database ---------------------------
run_check audit cargo-audit \
  cargo audit --json

# --- 3. cargo-deny — licenses, dupes, banned crates, advisories -----------
run_check deny cargo-deny \
  cargo deny --format json check

# --- 4. cargo-outdated — out-of-date workspace deps ------------------------
run_check outdated cargo-outdated \
  cargo outdated --workspace --format json

# --- 5. cargo-bloat — release-build size by crate -------------------------
run_check bloat cargo-bloat \
  cargo bloat --release --crates -n 30 --message-format json

# --- 6. cargo-machete — unused workspace deps -----------------------------
# Machete prints plain text (no JSON mode); we parse it after the run.
run_check machete cargo-machete \
  cargo machete

# === extract outdated findings (JSON Lines format) =========================
# cargo-outdated outputs one JSON object per workspace member, each with a
# .crate_name and .dependencies array. We extract actionable updates where
# .compat (semver-compatible) or .latest (any) indicates an available upgrade.
if [[ -s "$OUT/outdated.raw" ]]; then
  jq -s '
    map(
      {crate: .crate_name, deps: [.dependencies[] | select(
        (.compat != null and .compat != "" and .compat != "Removed" and .compat != "---")
        or (.latest != null and .latest != "" and .latest != "Removed" and .latest != "---")
      )]}
    ) |
    map(. + {update_count: (.deps | length)})
  ' "$OUT/outdated.raw" > "$OUT/outdated.findings.json" 2>/dev/null \
    || echo '[]' > "$OUT/outdated.findings.json"

  # Classify compat vs major updates by scanning all dep entries
  jq -s '
    [.[].dependencies[]] |
    {
      compat_updates: [.[] | select(.compat != null and .compat != "" and .compat != "Removed" and .compat != "---")],
      major_updates:  [.[] | select(
        (.compat == null or .compat == "" or .compat == "---" or .compat == "Removed")
        and (.latest != null and .latest != "" and .latest != "Removed" and .latest != "---")
      )]
    } |
    {compat_count: (.compat_updates | length), major_count: (.major_updates | length)}
  ' "$OUT/outdated.raw" > "$OUT/outdated.classification.json" 2>/dev/null \
    || echo '{}' > "$OUT/outdated.classification.json"

  jq --slurpfile f "$OUT/outdated.findings.json" \
     --slurpfile c "$OUT/outdated.classification.json" '
    . + {
      by_crate:            $f[0],
      total_outdated:      ($f[0] | map(.update_count) | add // 0),
      compat_updates:      $c[0].compat_count,
      major_updates:       $c[0].major_count,
      affected_crates:     [$f[0][] | select(.update_count > 0) | .crate]
    }
  ' "$OUT/outdated.summary.json" > "$OUT/.tmp" && mv "$OUT/.tmp" "$OUT/outdated.summary.json"
fi

# === extract bloat crate-size breakdown ====================================
# cargo-bloat --message-format json outputs a single JSON object:
#   {"file-size": N, "text-section-size": N, "crates": [{"name":.., "size":..}, ...]}
if [[ -s "$OUT/bloat.raw" ]]; then
  jq '{
    file_size_bytes:      .["file-size"],
    text_section_bytes:   .["text-section-size"],
    total_crates:         (.crates | length),
    top_crates:           [.crates[] | {name, size}] | sort_by(-.size)
  }' "$OUT/bloat.raw" > "$OUT/bloat.findings.json" 2>/dev/null \
    || echo '{}' > "$OUT/bloat.findings.json"

  jq --slurpfile f "$OUT/bloat.findings.json" '. + $f[0]' \
    "$OUT/bloat.summary.json" > "$OUT/.tmp" && mv "$OUT/.tmp" "$OUT/bloat.summary.json"
fi

# --- 7. cargo-geiger — unsafe footprint over the dep tree (slow) ----------
# Geiger refuses virtual manifests; on a workspace we iterate per member and
# assemble per-package JSON reports into one aggregate array.
run_geiger() {
  if ! command -v cargo-geiger >/dev/null 2>&1; then
    jq -n '{status:"not_installed", probe:"cargo-geiger"}' \
      > "$OUT/geiger.summary.json"
    log "skip   geiger (missing: cargo-geiger — try: cargo install cargo-geiger)"
    return
  fi

  if ! is_virtual_manifest; then
    run_check geiger cargo-geiger cargo geiger --output-format Json --quiet
    return
  fi

  log "run    geiger (per-member, virtual manifest)"
  local t0; t0=$(date +%s)
  local rc=0
  : > "$OUT/geiger.err"
  printf '[' > "$OUT/geiger.raw"
  local first=1
  while IFS=$'\t' read -r pkg manifest; do
    [[ -n "$pkg" ]] || continue
    log "       geiger: $pkg"
    local per_pkg="$OUT/geiger.${pkg}.raw"
    [[ $first -eq 0 ]] && printf ',' >> "$OUT/geiger.raw"
    first=0
    if cargo geiger --output-format Json --quiet \
         --manifest-path "$manifest" \
         > "$per_pkg" 2>> "$OUT/geiger.err"; then
      printf '{"package":%s,"report":' \
        "$(jq -Rn --arg s "$pkg" '$s')" >> "$OUT/geiger.raw"
      if [[ -s "$per_pkg" ]]; then
        cat "$per_pkg" >> "$OUT/geiger.raw"
      else
        printf 'null' >> "$OUT/geiger.raw"
      fi
      printf '}' >> "$OUT/geiger.raw"
    else
      rc=$?
      printf '{"package":%s,"error":"exit_%d"}' \
        "$(jq -Rn --arg s "$pkg" '$s')" "$rc" >> "$OUT/geiger.raw"
    fi
  done < <(list_workspace_members)
  printf ']\n' >> "$OUT/geiger.raw"
  local t1; t1=$(date +%s)

  # $rc is non-zero if ANY per-member run failed (set by `rc=$?` in the loop's
  # else branch; the success branch never resets it). Derive `status` the same
  # way run_check does so consumers checking only `status` don't see "ok" for
  # a failed run.
  local status; [[ $rc -eq 0 ]] && status=ok || status="exit_${rc}"

  jq -n \
    --arg status "$status" \
    --argjson rc  "$rc" \
    --argjson dur "$((t1-t0))" \
    --arg raw     "$OUT/geiger.raw" \
    --arg err     "$OUT/geiger.err" \
    '{status:$status, exit_code:$rc, duration_seconds:$dur, raw:$raw, err:$err, mode:"per-member"}' \
    > "$OUT/geiger.summary.json"
}
run_geiger

# === extract clippy lint-frequency summary ================================
if [[ -s "$OUT/clippy.raw" ]]; then
  jq -s '
    map(
      select(.reason == "compiler-message") |
      .message |
      select(.code != null and (.spans | length) > 0) |
      {lint: .code.code, level: .level}
    ) |
    group_by(.lint) |
    map({lint: .[0].lint, level: .[0].level, count: length}) |
    sort_by(-.count)
  ' "$OUT/clippy.raw" > "$OUT/clippy.lints.json" 2>/dev/null \
    || echo '[]' > "$OUT/clippy.lints.json"

  jq --slurpfile lints "$OUT/clippy.lints.json" '
    . + {
      by_lint:        $lints[0],
      distinct_lints: ($lints[0] | length),
      total_findings: ($lints[0] | map(.count) | add // 0)
    }
  ' "$OUT/clippy.summary.json" > "$OUT/.tmp" && mv "$OUT/.tmp" "$OUT/clippy.summary.json"
fi

# === extract machete unused-deps from plain-text output ===================
# Machete output formats across versions, defensive parser handles both:
#
#   crate-name -- /path/Cargo.toml:
#           unused-dep-1
#           unused-dep-2
#
# and the older single-line per-dep variant. We strip ANSI just in case
# NO_COLOR didn't take, then group by crate.
if [[ -s "$OUT/machete.raw" ]]; then
  sed -E 's/\x1B\[[0-9;]*[mGKHJ]//g' "$OUT/machete.raw" \
    | awk '
        # Header lines we want to skip outright.
        /^cargo-machete (found|finished|did)/ { next }
        /^If you believe cargo-machete/        { exit }
        /^$/                                    { next }

        # Crate header: "<crate> -- /abs/path/Cargo.toml:"
        /^[A-Za-z0-9_.-]+ -- .*Cargo\.toml:?$/ {
          # First whitespace-separated token is the crate name.
          crate = $1
          next
        }

        # Indented dep entry under a known crate.
        crate != "" && /^[[:space:]]+[A-Za-z0-9_-]+[[:space:]]*$/ {
          dep = $1
          printf "%s\t%s\n", crate, dep
        }
      ' \
    | jq -Rn '
        [inputs
          | select(length > 0)
          | split("\t")
          | {crate: .[0], dep: .[1]}]
        | group_by(.crate)
        | map({crate: .[0].crate, unused: map(.dep)})
      ' > "$OUT/machete.findings.json" 2>/dev/null \
    || echo '[]' > "$OUT/machete.findings.json"

  jq --slurpfile f "$OUT/machete.findings.json" '
    . + {
      unused_by_crate: $f[0],
      total_unused:    ($f[0] | map(.unused | length) | add // 0),
      affected_crates: ($f[0] | length)
    }
  ' "$OUT/machete.summary.json" > "$OUT/.tmp" && mv "$OUT/.tmp" "$OUT/machete.summary.json"
fi

# === extract audit findings summary ========================================
if [[ -s "$OUT/audit.raw" ]]; then
  jq '{
    vulnerabilities: (.vulnerabilities.count // 0),
    warnings:        ((.warnings // {} | to_entries | map(.value | length) | add) // 0)
  }' "$OUT/audit.raw" > "$OUT/audit.findings.json" 2>/dev/null \
    || echo '{}' > "$OUT/audit.findings.json"

  jq --slurpfile f "$OUT/audit.findings.json" '. + $f[0]' \
    "$OUT/audit.summary.json" > "$OUT/.tmp" && mv "$OUT/.tmp" "$OUT/audit.summary.json"
fi

# === assemble master report ================================================
commit="$(git -C "$TARGET" rev-parse --short HEAD 2>/dev/null || echo unknown)"
branch="$(git -C "$TARGET" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
rustc_v="$(rustc --version 2>/dev/null || echo unknown)"

jq -n \
  --arg ts     "$TS" \
  --arg repo   "$TARGET" \
  --arg commit "$commit" \
  --arg branch "$branch" \
  --arg rustc  "$rustc_v" \
  --slurpfile clippy   "$OUT/clippy.summary.json" \
  --slurpfile audit    "$OUT/audit.summary.json" \
  --slurpfile deny     "$OUT/deny.summary.json" \
  --slurpfile outdated "$OUT/outdated.summary.json" \
  --slurpfile bloat    "$OUT/bloat.summary.json" \
  --slurpfile machete  "$OUT/machete.summary.json" \
  --slurpfile geiger   "$OUT/geiger.summary.json" \
  '{
    timestamp: $ts,
    repo:      $repo,
    commit:    $commit,
    branch:    $branch,
    rustc:     $rustc,
    checks: {
      clippy:   $clippy[0],
      audit:    $audit[0],
      deny:     $deny[0],
      outdated: $outdated[0],
      bloat:    $bloat[0],
      machete:  $machete[0],
      geiger:   $geiger[0]
    }
  }' > "$OUT/report.json"

# Brief stderr summary so a human invoking the script gets a quick sense.
log "summary:"
jq -r '
  .checks | to_entries[] |
  "  \(.key | . + (" " * (10 - length))): status=\(.value.status)"
  + (if .value.total_findings   != null then "  findings=\(.value.total_findings)"   else "" end)
  + (if .value.vulnerabilities  != null then "  vulns=\(.value.vulnerabilities)"     else "" end)
  + (if .value.warnings         != null then "  warns=\(.value.warnings)"            else "" end)
  + (if .value.total_unused     != null then "  unused=\(.value.total_unused)"       else "" end)
  + (if .value.total_outdated   != null then "  outdated=\(.value.total_outdated)"   else "" end)
  + (if .value.compat_updates   != null then "  compat=\(.value.compat_updates)"     else "" end)
  + (if .value.major_updates    != null then "  major=\(.value.major_updates)"       else "" end)
  + (if .value.total_crates     != null then "  bloat_crates=\(.value.total_crates)" else "" end)
  + (if .value.mode             != null then "  mode=\(.value.mode)"                 else "" end)
  + "  (\(.value.duration_seconds // 0)s)"
' "$OUT/report.json" >&2

log "report: $OUT/report.json"
echo "$OUT/report.json"
