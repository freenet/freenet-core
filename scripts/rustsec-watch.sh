#!/usr/bin/env bash
# RustSec advisory watch for the QUIC/TLS stack.
#
# Usage: scripts/rustsec-watch.sh scheduled
#        scripts/rustsec-watch.sh pr <base-ref>      (e.g. origin/main)
#
# Two lockfiles are audited (a finding is keyed on advisory id + crate + version):
#   1. Cargo.lock as is.
#   2. A synthetic lockfile naming the upstream crates behind each freenet-quinn*
#      fork crate PRESENT in the lock (scripts/rustsec-fork-upstream.map),
#      because the fork's renamed crates are invisible to RustSec. No fork in
#      the lock means no synthetic lock and no map needed.
#
# scheduled: fail (exit 1) on a vulnerability or unsound advisory against a
#            STACK crate that is not in scripts/rustsec-baseline.tsv, or whose
#            baseline entry has expired. Other crates are listed, not gating.
# pr:        fail (exit 1) only for findings (any crate) absent from <base-ref>.
#
# Every run first audits scripts/rustsec-watch-fixture.lock and fails unless the
# known advisory is reported, so a cargo-audit output change or an empty
# advisory database cannot turn the watch into a silent green.
#
# Env: RUSTSEC_FORK_MAP overrides the map file; RUSTSEC_TODAY overrides the
# date (tests). Exit: 0 ok, 1 findings, 2 usage, 3 tooling/config failure.
set -euo pipefail

mode="${1:-}"
base_ref="${2:-}"
root="$(git rev-parse --show-toplevel)"
map="${RUSTSEC_FORK_MAP:-$root/scripts/rustsec-fork-upstream.map}"
baseline="$root/scripts/rustsec-baseline.tsv"
today="${RUSTSEC_TODAY:-$(date -u +%F)}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

die() { echo "::error::$*"; exit 3; }

# Crates whose advisories matter to the transport/TLS path.
stack_re='^(quinn|quinn-proto|quinn-udp|freenet-quinn|freenet-quinn-proto|rustls|rustls-webpki|webpki|webpki-roots|rustls-pki-types|rustls-native-certs|rustls-pemfile|rustls-platform-verifier|tokio-rustls|ring|aws-lc-rs|aws-lc-sys|rcgen|ed25519-dalek|curve25519-dalek|x509-parser|spki|der)$'

# fork_lock <lockfile> <mapfile> <out>: writes a lockfile with the upstream
# crate for each freenet-quinn* package in <lockfile>. Returns 1 if there are none.
fork_lock() {
  local lock="$1" mapf="$2" out="$3" f n=0
  { echo 'version = 3'; echo
    for f in $(grep -oE '^name = "freenet-quinn[a-z-]*"' "$lock" | cut -d'"' -f2 | sort -u); do
      [ -f "$mapf" ] || die "$f is in the lockfile but $mapf does not exist; its upstream advisories are not being watched"
      local row; row="$(awk -v f="$f" '$1==f {print $2, $3}' "$mapf")"
      [ -n "$row" ] || die "$f is in the lockfile but not in $mapf; its upstream advisories are not being watched"
      read -r up ver <<<"$row"
      printf '[[package]]\nname = "%s"\nversion = "%s"\nsource = "registry+https://github.com/rust-lang/crates.io-index"\n\n' "$up" "$ver"
      n=$((n + 1))
    done
  } >"$out"
  [ "$n" -gt 0 ]
}

# audit <lockfile> <out.json>: cargo-audit exits 1 for vulnerabilities; any
# other failure (no DB, parse error) fails the job.
audit() {
  local rc=0
  cargo audit --file "$1" --json >"$2" 2>"$2.err" || rc=$?
  if [ "$rc" -gt 1 ] || ! jq -e '.vulnerabilities.list' "$2" >/dev/null 2>&1; then
    cat "$2.err"; die "cargo audit failed to run on $1 (exit $rc)"
  fi
}

# findings <json> <label>: TSV rows id, crate, version, kind, label, title.
findings() {
  jq -r --arg l "$2" '
    (.vulnerabilities.list[]? | [.advisory.id, .package.name, .package.version, "vulnerability", $l, .advisory.title]),
    (.warnings.unsound[]?     | [.advisory.id, .package.name, .package.version, "unsound", $l, .advisory.title])
    | @tsv' "$1"
}

# collect <lockfile> <mapfile> <label-prefix> <out.tsv>: all findings for one
# tree (its lock plus the upstream crates of its forks).
collect() {
  local lock="$1" mapf="$2" out="$4"
  audit "$lock" "$work/c.json"
  findings "$work/c.json" "Cargo.lock" >"$out"
  if fork_lock "$lock" "$mapf" "$work/c-fork.lock"; then
    audit "$work/c-fork.lock" "$work/c-fork.json"
    findings "$work/c-fork.json" "fork-upstream" >>"$out"
  fi
}

# One row per (id, crate, version): the same finding from both lockfiles is one.
dedupe() { sort -t$'\t' -k1,3 -u "$1" >"$1.u" && mv "$1.u" "$1"; }

case "$mode" in scheduled | pr) ;; *) echo "usage: $0 scheduled | pr <base-ref>"; exit 2 ;; esac
[ "$mode" = pr ] && [ -z "$base_ref" ] && { echo "pr mode needs a base ref"; exit 2; }

# Self-test: the watch must be able to see a known advisory.
audit "$root/scripts/rustsec-watch-fixture.lock" "$work/fixture.json"
findings "$work/fixture.json" fixture | cut -f1 | grep -qx RUSTSEC-2023-0063 \
  || die "self-test failed: scripts/rustsec-watch-fixture.lock should report RUSTSEC-2023-0063 and did not (cargo-audit output changed, or the advisory database is empty)"

collect "$root/Cargo.lock" "$map" x "$work/all.tsv"
dedupe "$work/all.tsv"
echo "findings (vulnerability or unsound): $(wc -l <"$work/all.tsv")"

if [ "$mode" = pr ]; then
  git -C "$root" show "$base_ref:Cargo.lock" >"$work/base.lock" || die "cannot read Cargo.lock at $base_ref"
  git -C "$root" show "$base_ref:scripts/rustsec-fork-upstream.map" >"$work/base.map" 2>/dev/null || cp "$map" "$work/base.map"  # map not on base yet (the PR adding it)
  collect "$work/base.lock" "$work/base.map" x "$work/base.tsv"
  cut -f1-3 "$work/base.tsv" | sort -u >"$work/base.keys"
  new=0
  while IFS=$'\t' read -r id name ver kind label title; do
    printf '%s\t%s\t%s\n' "$id" "$name" "$ver" | grep -qxFf "$work/base.keys" && continue
    echo "::error::new finding introduced by this PR: $id $name $ver ($kind, $label): $title"; new=1
  done <"$work/all.tsv"
  [ "$new" -eq 0 ] && echo "no new findings relative to $base_ref"
  exit "$new"
fi

# scheduled
awk -F'\t' -v re="$stack_re" '$2 !~ re' "$work/all.tsv" >"$work/other.tsv"
echo "== other crates (not gating): $(wc -l <"$work/other.tsv")"
cut -f1-3 "$work/other.tsv" | sed 's/^/  /'
awk -F'\t' -v re="$stack_re" '$2 ~ re' "$work/all.tsv" >"$work/stack.tsv"
grep -vE '^(#|$)' "$baseline" >"$work/baseline.tsv" || true
status=0
echo "== QUIC/TLS stack: $(wc -l <"$work/stack.tsv") finding(s), $(wc -l <"$work/baseline.tsv") baseline entr(ies)"
while IFS=$'\t' read -r id name ver kind label title; do
  entry="$(awk -F'\t' -v i="$id" -v n="$name" -v v="$ver" '$1==i && $2==n && $3==v' "$work/baseline.tsv" | head -1)"
  if [ -z "$entry" ]; then
    echo "::error::NEW stack advisory $id $name $ver ($kind, $label): $title"; status=1; continue
  fi
  expires="$(cut -f5 <<<"$entry")"; track="$(cut -f4 <<<"$entry")"
  if [[ ! "$expires" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ || -z "$track" ]]; then
    echo "::error::baseline entry for $id $name $ver needs a tracking link and a YYYY-MM-DD expiry"; status=1
  elif [[ "$today" > "$expires" ]]; then
    echo "::error::baseline entry for $id $name $ver EXPIRED on $expires (tracking: $track)"; status=1
  else
    echo "known: $id $name $ver, tracked in $track, expires $expires"
  fi
done <"$work/stack.tsv"
# Baseline entries that no longer match anything.
while IFS=$'\t' read -r id name ver _; do
  printf '%s\t%s\t%s\n' "$id" "$name" "$ver" | grep -qxFf <(cut -f1-3 "$work/stack.tsv") \
    || echo "::warning::baseline entry $id $name $ver matches nothing any more: delete it from scripts/rustsec-baseline.tsv"
done <"$work/baseline.tsv"
exit "$status"
