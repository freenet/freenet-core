#!/usr/bin/env bash
# RustSec advisory watch for the QUIC/TLS stack.
#
# Usage: scripts/rustsec-watch.sh scheduled
#        scripts/rustsec-watch.sh pr <base-ref>      (e.g. origin/main)
#
# Two lockfiles are audited:
#   1. Cargo.lock as is.
#   2. A synthetic lockfile naming the upstream crates the quinn fork is based
#      on (scripts/rustsec-fork-upstream.map), because the fork's renamed crates
#      are invisible to RustSec.
#
# scheduled: exit 1 on any vulnerability or unsound advisory against a STACK
#            crate. Advisories on other crates are listed but do not fail: the
#            workspace has unrelated pre-existing ones, and a job that is red
#            every day stops being read.
# pr:        exit 1 only for advisories that are NOT present in <base-ref>'s
#            Cargo.lock, on any crate. A PR is never blocked by an advisory it
#            did not introduce.
#
# Env: RUSTSEC_FORK_MAP overrides the map file (used to test detection).
set -euo pipefail

mode="${1:?mode: scheduled|pr}"
base_ref="${2:-}"
root="$(git rev-parse --show-toplevel)"
map="${RUSTSEC_FORK_MAP:-$root/scripts/rustsec-fork-upstream.map}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Crates whose advisories matter to the transport/TLS path.
stack_re='^(quinn|quinn-proto|quinn-udp|freenet-quinn|freenet-quinn-proto|rustls|rustls-webpki|webpki|rustls-pki-types|rustls-native-certs|tokio-rustls|ring|rcgen|ed25519-dalek|curve25519-dalek|x509-parser|spki|der)$'

# Writes a lockfile holding only the mapped upstream crates (no dependency
# edges are needed: cargo-audit matches by name and version).
make_fork_lock() {
  local out="$1"
  { echo 'version = 3'; echo
    while read -r fork up ver; do
      case "$fork" in ''|'#'*) continue ;; esac
      printf '[[package]]\nname = "%s"\nversion = "%s"\nsource = "registry+https://github.com/rust-lang/crates.io-index"\n\n' "$up" "$ver"
    done <"$map"
  } >"$out"
}

# Fail closed if the lock has a fork crate the map does not describe, so a new
# fork crate cannot silently escape the watch.
check_map_covers_lock() {
  local lock="$1" f
  for f in $(grep -oE '^name = "freenet-quinn[a-z-]*"' "$lock" | cut -d'"' -f2 | sort -u); do
    grep -qE "^$f " "$map" || { echo "::error::$f is in Cargo.lock but not in $map; its upstream advisories are not being watched"; exit 1; }
  done
}

# audit <lockfile> <out.json>: raw cargo-audit JSON. cargo-audit exits 1 for
# vulnerabilities; any other failure (no DB, parse error) must fail the job.
audit() {
  local rc=0
  cargo audit --file "$1" --json >"$2" 2>"$2.err" || rc=$?
  if [ "$rc" -gt 1 ] || ! jq -e .vulnerabilities "$2" >/dev/null 2>&1; then
    echo "::error::cargo audit failed to run on $1 (exit $rc)"; cat "$2.err"; exit 1
  fi
}

# findings <json> <label>: one TSV row per vulnerability or unsound warning.
findings() {
  jq -r --arg l "$2" '
    (.vulnerabilities.list[]? | [.advisory.id, .package.name, .package.version, "vulnerability", $l, .advisory.title]),
    (.warnings.unsound[]?     | [.advisory.id, .package.name, .package.version, "unsound", $l, .advisory.title])
    | @tsv' "$1"
}

check_map_covers_lock "$root/Cargo.lock"
make_fork_lock "$work/fork.lock"
audit "$root/Cargo.lock" "$work/main.json"
audit "$work/fork.lock" "$work/fork.json"
{ findings "$work/main.json" "Cargo.lock"; findings "$work/fork.json" "fork-upstream"; } | sort -u >"$work/all.tsv"
echo "advisories (vulnerability or unsound): $(wc -l <"$work/all.tsv") in total"

case "$mode" in
scheduled)
  awk -F'\t' -v re="$stack_re" '$2 ~ re' "$work/all.tsv" >"$work/stack.tsv"
  awk -F'\t' -v re="$stack_re" '$2 !~ re' "$work/all.tsv" >"$work/other.tsv"
  echo "== other crates (not gating): $(wc -l <"$work/other.tsv")"
  cut -f1-3 "$work/other.tsv" | sed 's/^/  /'
  echo "== QUIC/TLS stack (gating): $(wc -l <"$work/stack.tsv")"
  if [ -s "$work/stack.tsv" ]; then
    while IFS=$'\t' read -r id name ver kind label title; do
      echo "::error::$id $name $ver ($kind, $label): $title"
    done <"$work/stack.tsv"
    exit 1
  fi ;;
pr)
  [ -n "$base_ref" ] || { echo "pr mode needs a base ref"; exit 2; }
  git -C "$root" show "$base_ref:Cargo.lock" >"$work/base.lock"
  audit "$work/base.lock" "$work/base.json"
  findings "$work/base.json" "Cargo.lock" | cut -f1 | sort -u >"$work/base.ids"
  # Fork-upstream findings in a PR come from the map file, whose base version
  # is the one on <base-ref>.
  if git -C "$root" show "$base_ref:scripts/rustsec-fork-upstream.map" >"$work/base.map" 2>/dev/null; then
    saved_map="$map"; map="$work/base.map"; make_fork_lock "$work/base-fork.lock"; map="$saved_map"
    audit "$work/base-fork.lock" "$work/base-fork.json"
    findings "$work/base-fork.json" "fork-upstream" | cut -f1 | sort -u >>"$work/base.ids"
    sort -u -o "$work/base.ids" "$work/base.ids"
  fi
  new=0
  while IFS=$'\t' read -r id name ver kind label title; do
    grep -qx "$id" "$work/base.ids" && continue
    echo "::error::new advisory introduced by this PR: $id $name $ver ($kind, $label): $title"; new=1
  done <"$work/all.tsv"
  [ "$new" -eq 0 ] && echo "no new advisories relative to $base_ref" ;;
*) echo "unknown mode $mode"; exit 2 ;;
esac
