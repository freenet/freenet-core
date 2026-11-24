#!/usr/bin/env bash
# Tests for fetch-release-binary.sh: release selection, floor parsing and the
# checksum gate, all against local fixtures (no network).
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/fetch-release-binary.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
fails=0

check() { # check DESCRIPTION ACTUAL EXPECTED
    if [ "$2" = "$3" ]; then
        echo "ok   - $1"
    else
        echo "FAIL - $1"
        echo "  got:      $2"
        echo "  expected: $3"
        fails=$((fails + 1))
    fi
}

sel() { printf '%s\n' "$1" | bash "$script" select "${2:-}" | tr '\n' ' '; }

# Semver order, not lexical: 0.2.99 < 0.2.100.
check "semver, not lexical, order" \
    "$(sel $'v0.2.99\nv0.2.100\nv0.2.98')" "n1=v0.2.100 n2=v0.2.99 "
# Input order and duplicates do not matter.
check "unordered input with duplicates" \
    "$(sel $'v0.2.5\nv0.2.7\nv0.2.7\nv0.2.6')" "n1=v0.2.7 n2=v0.2.6 "
# Junk tags are ignored (prerelease-looking, no v, extra components).
check "non-release tags ignored" \
    "$(sel $'v0.2.9-rc1\nlatest\n0.2.9\nv0.2.8\nv0.2.7\nv0.2.7.1')" "n1=v0.2.8 n2=v0.2.7 "
# The tag under test is excluded when named; otherwise it is a normal release.
check "excluded tag skipped" \
    "$(sel $'v0.2.9\nv0.2.8\nv0.2.7' v0.2.9)" "n1=v0.2.8 n2=v0.2.7 "
check "same version as HEAD is not excluded without --exclude-tag" \
    "$(sel $'v0.2.9\nv0.2.8\nv0.2.7')" "n1=v0.2.9 n2=v0.2.8 "
check "exclusion is exact, not a prefix" \
    "$(sel $'v0.2.10\nv0.2.1\nv0.2.0' v0.2.1)" "n1=v0.2.10 n2=v0.2.0 "
check "only one release gives only n1" \
    "$(sel 'v1.0.0')" "n1=v1.0.0 "
check "1.0.0 sorts above 0.2.x" \
    "$(sel $'v0.2.200\nv1.0.0\nv0.9.0')" "n1=v1.0.0 n2=v0.9.0 "
check "no releases gives nothing" "$(sel '')" ""

# Floor parsing.
floor() { printf '%s\n' "$1" | bash "$script" floor-from-cargo-toml; }
check "floor from metadata" \
    "$(floor $'[package]\nname = "freenet"\nversion = "0.2.163"\n[package.metadata.freenet]\nmin-compatible-version = "0.2.122"')" \
    "0.2.122"
check "floor falls back to package version" \
    "$(floor $'[package]\nname = "freenet"\nversion = "0.2.150"')" "0.2.150"
if printf '' | bash "$script" floor-from-cargo-toml >/dev/null 2>&1; then
    echo "FAIL - empty Cargo.toml must not yield a floor"
    fails=$((fails + 1))
else
    echo "ok   - empty Cargo.toml yields no floor"
fi

# Checksum gate, with file:// fixtures standing in for GitHub.
mk_release() { # mk_release TAG [tamper]
    local tag="$1" d="$work/dl/$1"
    mkdir -p "$d" "$work/pkg-$tag"
    printf '#!/bin/sh\necho %s\n' "$tag" >"$work/pkg-$tag/freenet"
    tar -czf "$d/freenet-x86_64-unknown-linux-musl.tar.gz" -C "$work/pkg-$tag" freenet
    (cd "$d" && sha256sum freenet-x86_64-unknown-linux-musl.tar.gz >SHA256SUMS.txt)
    if [ "${2:-}" = tamper ]; then
        printf 'x' >>"$d/freenet-x86_64-unknown-linux-musl.tar.gz"
    fi
    if [ "${2:-}" = noentry ]; then
        echo "deadbeef  something-else.tar.gz" >"$d/SHA256SUMS.txt"
    fi
}
mk_release v0.2.19
mk_release v0.2.20
mk_release v0.2.21
mk_release v0.2.22 tamper
mk_release v0.2.23 noentry
mk_release v0.2.14
mk_release v0.2.15 tamper
mk_release v0.2.16 noentry
mk_release v0.2.30
mk_release v0.2.31
export FREENET_RELEASE_BASE_URL="file://$work/dl"

out="$work/out"
path="$(bash "$script" fetch v0.2.21 "$out" 2>/dev/null)"
check "verified download is extracted and executable" "$("$path")" "v0.2.21"
check "second fetch is a cache hit" "$(bash "$script" fetch v0.2.21 "$out" 2>&1 >/dev/null)" "cache hit for v0.2.21"

if bash "$script" fetch v0.2.22 "$out" >/dev/null 2>"$work/err"; then
    echo "FAIL - tampered archive was accepted"
    fails=$((fails + 1))
else
    check "tampered archive rejected" "$(grep -c 'checksum mismatch' "$work/err")" "1"
fi
if [ -e "$out/freenet-0.2.22" ]; then
    echo "FAIL - tampered archive left a binary behind"
    fails=$((fails + 1))
else
    echo "ok   - no binary left after a rejected download"
fi
bash "$script" fetch v0.2.23 "$out" >/dev/null 2>"$work/err"
check "missing checksum entry exits 3 (skippable: no asset)" "$?" "3"
check "missing checksum entry says why" "$(grep -c 'no entry' "$work/err")" "1"
bash "$script" fetch v0.2.22 "$out" >/dev/null 2>"$work/err"
check "checksum mismatch exits 1 (never skippable)" "$?" "1"
bash "$script" fetch v0.2.99 "$out" >/dev/null 2>"$work/err"
check "release with no files exits 3" "$?" "3"
# A cached binary whose archive checksum changed upstream is not reused.
sed -i '1s/.*/stale/' "$out/freenet-0.2.21.sha256"
check "stale cache record forces a re-download" \
    "$(bash "$script" fetch v0.2.21 "$out" 2>&1 >/dev/null | grep -c 'cache hit')" "0"
check "cache is valid again after the re-download" \
    "$(bash "$script" fetch v0.2.21 "$out" 2>&1 >/dev/null | grep -c 'cache hit')" "1"
# A cached binary that no longer hashes to the recorded value is not reused.
printf 'tampered' >>"$out/freenet-0.2.21"
check "modified cached binary forces a re-download" \
    "$(bash "$script" fetch v0.2.21 "$out" 2>&1 >/dev/null | grep -c 'cache hit')" "0"
check "re-download restored the binary" "$("$out/freenet-0.2.21")" "v0.2.21"
if bash "$script" fetch 'v0.2.21;rm' "$out" >/dev/null 2>&1; then
    echo "FAIL - odd tag accepted"
    fails=$((fails + 1))
else
    echo "ok   - odd tag refused"
fi

# resolve end to end: manifest content.
printf 'v0.2.21\nv0.2.20\nv0.2.19\n' >"$work/tags"
printf '[package]\nversion = "0.2.21"\n[package.metadata.freenet]\nmin-compatible-version = "0.2.10"\n' >"$work/Cargo.toml"
export FREENET_RELEASE_TAGS_FILE="$work/tags"
export FREENET_CARGO_TOML_URL_TEMPLATE="file://$work/Cargo.toml"
bash "$script" resolve "$work/res" >/dev/null 2>&1
check "manifest n1 tag" "$(jq -r .n1.tag "$work/res/versions.json")" "v0.2.21"
check "manifest n2 tag" "$(jq -r .n2.tag "$work/res/versions.json")" "v0.2.20"
check "manifest floor" "$(jq -r .n1.floor "$work/res/versions.json")" "0.2.10"
check "manifest path runs" "$("$(jq -r .n2.path "$work/res/versions.json")")" "v0.2.20"
bash "$script" resolve "$work/res2" --exclude-tag v0.2.21 >/dev/null 2>&1
check "resolve honours --exclude-tag" "$(jq -r .n1.tag "$work/res2/versions.json")" "v0.2.20"

# Below-floor selection: strictly below, newest first, floor itself excluded.
below() { (source "$script"; printf '%s\n' "$1" | below_candidates "$2" | tr '\n' ' '); }
check "below-floor candidates are strictly below, newest first" \
    "$(below $'v0.2.122\nv0.2.121\nv0.2.99\nv0.2.123\nv0.2.100' 0.2.122)" "v0.2.121 v0.2.100 v0.2.99 "
check "below-floor with nothing below" "$(below $'v0.2.5\nv0.2.6' 0.2.5)" ""

printf '[package]\nversion = "0.2.21"\n[package.metadata.freenet]\nmin-compatible-version = "0.2.20"\n' >"$work/head-Cargo.toml"
bash "$script" resolve "$work/res3" --core-toml "$work/head-Cargo.toml" >/dev/null 2>&1
check "manifest below tag is the newest under the floor" "$(jq -r .below.tag "$work/res3/versions.json")" "v0.2.19"
check "manifest below binary runs" "$("$(jq -r .below.path "$work/res3/versions.json")")" "v0.2.19"
# A below-floor candidate that has no asset (no checksum entry, or no files at
# all) is skipped for an older one. A checksum mismatch is a failure, not a skip.
printf '[package]\nversion = "0.2.23"\n[package.metadata.freenet]\nmin-compatible-version = "0.2.20"\n' >"$work/head-Cargo.toml"
printf 'v0.2.31\nv0.2.30\nv0.2.17\nv0.2.16\nv0.2.14\n' >"$work/tags"
bash "$script" resolve "$work/res4" --core-toml "$work/head-Cargo.toml" >/dev/null 2>&1
check "below-floor candidates without assets are skipped" "$(jq -r .below.tag "$work/res4/versions.json")" "v0.2.14"
printf 'v0.2.31\nv0.2.30\nv0.2.15\nv0.2.14\n' >"$work/tags"
if bash "$script" resolve "$work/res5" --core-toml "$work/head-Cargo.toml" >/dev/null 2>"$work/err"; then
    echo "FAIL - a tampered below-floor candidate was skipped"
    fails=$((fails + 1))
else
    check "tampered below-floor candidate fails the resolve" "$(grep -c 'checksum mismatch' "$work/err")" "1"
fi
if [ -e "$work/res5/versions.json" ]; then
    echo "FAIL - versions.json written after a failed resolve"
    fails=$((fails + 1))
else
    echo "ok   - no manifest after a failed resolve"
fi

if [ "$fails" -ne 0 ]; then
    echo "$fails check(s) failed"
    exit 1
fi
echo "all checks passed"
