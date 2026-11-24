#!/usr/bin/env bash
# Resolve and download the released freenet binaries the mixed-version test
# runs against, and write a manifest the test reads.
#
#   fetch-release-binary.sh resolve OUT_DIR [--exclude-tag TAG] [--core-toml PATH]
#       Pick N-1 and N-2 from the published GitHub releases, download and
#       verify their linux x86_64 musl binaries into OUT_DIR, and write
#       OUT_DIR/versions.json. With --core-toml (crates/core/Cargo.toml of the
#       build under test) it also fetches "below": the newest release strictly
#       below that build's min-compatible-version, which N must refuse.
#   fetch-release-binary.sh select [EXCLUDE_TAG]
#       Read tags on stdin, print "n1=<tag>" and "n2=<tag>" (selection only).
#   fetch-release-binary.sh tags [--exclude-tag TAG]
#       List the published releases and print "n1=<tag>" / "n2=<tag>" without
#       downloading (used to key the CI cache).
#   fetch-release-binary.sh fetch TAG OUT_DIR
#       Download, verify and cache one release binary; print its path.
#   fetch-release-binary.sh floor TAG
#       Print the min-compatible version the release at TAG was built with.
#
# Definitions. N-1 is the newest published (not draft, not prerelease) release,
# N-2 the one before it, both compared by semantic version, never by date. A
# release is skipped only when it is named with --exclude-tag. That option is
# reserved for a future release gate (the tag under test) and is not used by the
# nightly or PR workflow. Version strings of the HEAD build
# are never compared: between releases HEAD usually carries the same version
# as the newest release, and that release is still a valid, different binary.
#
# Every download is checked against the release's SHA256SUMS.txt. There is no
# unverified fallback: a missing or mismatching entry fails the run.
#
# Environment:
#   GITHUB_TOKEN / GH_TOKEN   used for GitHub API and download requests
#   FREENET_REPO              owner/name (default freenet/freenet-core)
#   FREENET_RELEASE_BASE_URL  override of https://github.com/<repo>/releases/download
#                             (tests point it at file:// fixtures)
#   FREENET_RELEASE_TAGS_FILE read tags from this file instead of the API (tests)
#   FREENET_CARGO_TOML_URL_TEMPLATE
#                             override for the Cargo.toml fetch; "%TAG%" is replaced
set -euo pipefail

REPO="${FREENET_REPO:-freenet/freenet-core}"
ASSET="freenet-x86_64-unknown-linux-musl.tar.gz"
TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+$'

die() { echo "error: $*" >&2; exit 1; }

curl_auth() {
    local token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
    if [ -n "$token" ]; then
        curl -fsSL --retry 3 --retry-delay 2 -H "Authorization: Bearer $token" "$@"
    else
        curl -fsSL --retry 3 --retry-delay 2 "$@"
    fi
}

# Newest first, strict vX.Y.Z only, optionally without one excluded tag.
select_tags() {
    local exclude="${1:-}"
    local tags
    tags="$(grep -E "$TAG_RE" || true)"
    if [ -n "$exclude" ]; then
        tags="$(printf '%s\n' "$tags" | grep -vxF "$exclude" || true)"
    fi
    printf '%s\n' "$tags" | grep -E "$TAG_RE" | sort -u | sort -V -r | head -n 2 |
        awk 'NR==1{print "n1=" $0} NR==2{print "n2=" $0}'
}

# Published, non-draft, non-prerelease release tags, one per line.
list_release_tags() {
    if [ -n "${FREENET_RELEASE_TAGS_FILE:-}" ]; then
        cat "$FREENET_RELEASE_TAGS_FILE"
        return
    fi
    local page=1 out
    while [ "$page" -le 5 ]; do
        out="$(curl_auth "https://api.github.com/repos/$REPO/releases?per_page=100&page=$page")" ||
            die "listing releases failed"
        [ "$(jq 'length' <<<"$out")" -eq 0 ] && break
        jq -r '.[] | select(.draft == false and .prerelease == false) | .tag_name' <<<"$out"
        page=$((page + 1))
    done
}

# min-compatible-version from Cargo.toml text on stdin; falls back to the
# package version, the same order crates/core/build.rs uses without the env var.
floor_from_cargo_toml() {
    local text floor
    text="$(cat)"
    floor="$(sed -n 's/^min-compatible-version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' <<<"$text" | head -n 1)"
    if [ -z "$floor" ]; then
        floor="$(sed -n 's/^version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' <<<"$text" | head -n 1)"
    fi
    [ -n "$floor" ] || return 1
    printf '%s\n' "$floor"
}

release_floor() {
    local tag="$1" url
    url="${FREENET_CARGO_TOML_URL_TEMPLATE:-https://raw.githubusercontent.com/$REPO/%TAG%/crates/core/Cargo.toml}"
    url="${url//%TAG%/$tag}"
    curl_auth "$url" | floor_from_cargo_toml || die "cannot read the floor for $tag"
}

sha256_of() { sha256sum "$1" | awk '{print $1}'; }

# Fetch URL into FILE. Returns 0 on success, 3 when the file does not exist
# (HTTP 404, or a missing file:// fixture), and dies on anything else, so a
# network or server failure is never mistaken for "this release has no asset".
download() {
    local url="$1" file="$2" code rc=0
    local args=(-sSL --retry 3 --retry-delay 2 -o "$file" -w '%{http_code}')
    local token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
    if [ -n "$token" ]; then args+=(-H "Authorization: Bearer $token"); fi
    code="$(curl "${args[@]}" "$url")" || rc=$?
    if [ "$rc" -eq 37 ]; then return 3; fi # file:// source not readable
    [ "$rc" -eq 0 ] || die "download of $url failed (curl exit $rc)"
    case "$code" in
        200 | 000) return 0 ;;
        404) return 3 ;;
        *) die "download of $url returned HTTP $code" ;;
    esac
}

# Download, verify, extract. Prints the binary path.
#
# Exit codes: 0 ok; 3 the release has no musl asset or no checksum entry for it
# (the only condition callers may skip past); anything else is a failure. A
# checksum mismatch always dies.
#
# Cached by tag: a tag's assets do not change. A cache hit requires that the
# release's current SHA256SUMS.txt entry equals the recorded one AND that the
# cached binary hashes to the value recorded when it was installed. Install is
# atomic (temp file in the same directory, then mv), so a killed run cannot
# leave a half-written binary that a later run trusts.
fetch_release() (
    local tag="$1" out="$2" base work want got bin rc=0
    [[ "$tag" =~ $TAG_RE ]] || die "refusing odd tag '$tag'"
    base="${FREENET_RELEASE_BASE_URL:-https://github.com/$REPO/releases/download}/$tag"
    mkdir -p "$out"
    bin="$out/freenet-${tag#v}"

    work="$(mktemp -d)"
    trap 'rm -rf "$work"' EXIT
    download "$base/SHA256SUMS.txt" "$work/SHA256SUMS.txt" || rc=$?
    if [ "$rc" -eq 3 ]; then echo "$tag has no SHA256SUMS.txt" >&2; return 3; fi
    want="$(awk -v a="$ASSET" '$2 == a || $2 == "*" a {print $1}' "$work/SHA256SUMS.txt" | head -n 1)"
    if [ -z "$want" ]; then echo "SHA256SUMS.txt for $tag has no entry for $ASSET" >&2; return 3; fi

    if [ -f "$bin" ] && [ -f "$bin.sha256" ] &&
        [ "$(sed -n 1p "$bin.sha256")" = "$want" ] &&
        [ "$(sed -n 2p "$bin.sha256")" = "$(sha256_of "$bin")" ]; then
        echo "cache hit for $tag" >&2
        printf '%s\n' "$bin"
        return
    fi

    rc=0
    download "$base/$ASSET" "$work/$ASSET" || rc=$?
    if [ "$rc" -eq 3 ]; then echo "$tag has no $ASSET" >&2; return 3; fi
    got="$(sha256_of "$work/$ASSET")"
    [ "$got" = "$want" ] || die "checksum mismatch for $tag $ASSET: got $got, SHA256SUMS.txt says $want"

    mkdir "$work/x"
    tar -xzf "$work/$ASSET" -C "$work/x" || die "cannot extract $ASSET for $tag"
    [ -f "$work/x/freenet" ] || die "$ASSET for $tag has no top-level 'freenet' file"
    rm -f "$bin.sha256"
    install -m 0755 "$work/x/freenet" "$bin.tmp.$$"
    mv -f "$bin.tmp.$$" "$bin"
    printf '%s\n%s\n' "$want" "$(sha256_of "$bin")" >"$bin.sha256"
    printf '%s\n' "$bin"
)

# Tags strictly below version FLOOR (X.Y.Z), newest first, at most five. Tags on
# stdin. The first one with a verifiable musl asset is the "below the floor" build.
below_candidates() {
    local floor="$1"
    grep -E "$TAG_RE" | sort -u | while read -r t; do
        [ "${t#v}" = "$floor" ] && continue
        # Strictly below: it sorts first of the pair.
        [ "$(printf '%s\n%s\n' "${t#v}" "$floor" | sort -V | head -n 1)" = "${t#v}" ] && echo "$t"
    done | sort -V -r | head -n 5
}

resolve() {
    local out="$1" exclude="${2:-}" core_toml="${3:-}" sel n1 n2 role tag bin floor
    mkdir -p "$out"
    sel="$(list_release_tags | select_tags "$exclude")"
    n1="$(sed -n 's/^n1=//p' <<<"$sel")"
    n2="$(sed -n 's/^n2=//p' <<<"$sel")"
    [ -n "$n1" ] || die "no published release found to use as N-1"
    local json='{}'
    for role in n1 n2; do
        tag="${!role}"
        if [ -z "$tag" ]; then
            echo "no release available for $role; the test will skip that role" >&2
            continue
        fi
        bin="$(fetch_release "$tag" "$out")"
        floor="$(release_floor "$tag")"
        json="$(jq --arg r "$role" --arg tag "$tag" --arg ver "${tag#v}" --arg path "$(readlink -f "$bin")" \
            --arg sha "$(sha256_of "$bin")" --arg floor "$floor" \
            '.[$r] = {tag: $tag, version: $ver, path: $path, binary_sha256: $sha, floor: $floor}' <<<"$json")"
    done

    # The newest release strictly below the floor of the build under test, so a
    # refusal is exercised today and not only after the next floor raise.
    if [ -n "$core_toml" ]; then
        local head_floor cand
        head_floor="$(floor_from_cargo_toml <"$core_toml")" || die "cannot read the floor from $core_toml"
        for cand in $(list_release_tags | below_candidates "$head_floor"); do
            local rc=0
            bin="$(fetch_release "$cand" "$out")" || rc=$?
            if [ "$rc" -eq 3 ]; then
                echo "$cand has no musl asset or checksum entry, trying an older release" >&2
                continue
            fi
            [ "$rc" -eq 0 ] || die "fetching $cand failed (exit $rc)"
            floor="$(release_floor "$cand")"
            json="$(jq --arg tag "$cand" --arg ver "${cand#v}" --arg path "$(readlink -f "$bin")" \
                --arg sha "$(sha256_of "$bin")" --arg floor "$floor" \
                '.below = {tag: $tag, version: $ver, path: $path, binary_sha256: $sha, floor: $floor}' <<<"$json")"
            break
        done
        if [ "$(jq 'has("below")' <<<"$json")" != true ]; then
            echo "no release below floor $head_floor has a musl asset with a checksum entry" >&2
        fi
    fi
    printf '%s\n' "$json" >"$out/versions.json"
    echo "wrote $out/versions.json" >&2
    cat "$out/versions.json"
}

main() {
    local cmd="${1:-}"
    shift || true
    case "$cmd" in
        select) select_tags "${1:-}" ;;
        tags)
            local exclude=""
            if [ "${1:-}" = "--exclude-tag" ]; then exclude="${2:?tag}"; fi
            list_release_tags | select_tags "$exclude"
            ;;
        floor) release_floor "${1:?tag}" ;;
        floor-from-cargo-toml) floor_from_cargo_toml ;;
        fetch) fetch_release "${1:?tag}" "${2:?out dir}" ;;
        resolve)
            local out="${1:?out dir}" exclude="" core_toml=""
            shift
            while [ $# -gt 0 ]; do
                case "$1" in
                    --exclude-tag) exclude="${2:?tag}"; shift 2 ;;
                    --core-toml) core_toml="${2:?path}"; shift 2 ;;
                    *) die "unknown argument $1" ;;
                esac
            done
            resolve "$out" "$exclude" "$core_toml"
            ;;
        *) sed -n '2,24p' "$0"; exit 2 ;;
    esac
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    main "$@"
fi
