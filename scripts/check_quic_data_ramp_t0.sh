#!/usr/bin/env bash
# Check, when the QUIC data ramp release is cut, that its ramp start (T0) is
# close to now (#5940, #6138).
#
# T0 (`QUIC_DATA_RAMP_START` in crates/core/src/transport/quic/upgrade/policy.rs)
# is fixed in the binary. A publish long after T0 starts every node that updates
# part-way along the curve (24 h after T0, at 100%, skipping the ramp), and one
# long before T0 holds every node at 5% until T0. So the ramp release must be
# cut within WINDOW of T0, or T0 must be re-set in a new reviewed PR first.
#
# Only the release named by `QUIC_DATA_RAMP_RELEASE` (beside T0) is held to
# T0: T0 stays set after it (past 24 h it reads 100%, the steady state), and
# later releases must not be held to it, so one passes with a warning. A
# version that would ship the ramp unchecked fails closed like a late cut:
# T0 set with no release named; a version before the named one; a later one
# that skips it while this tree (crates/core/Cargo.toml) has not reached it;
# a later one while the ramp release is not a published (non-draft) GitHub
# release, GitHub's latest release is before it, or it is not on crates.io,
# or any of these has no clear answer (the bump can merge and the publish or
# un-draft then fail); a later one
# cut more than WINDOW before T0. With no T0, or no ramp release and no T0, it
# prints a notice and exits 0.
#
# Usage:
#   check_quic_data_ramp_t0.sh --version X.Y.Z --live true|false
#                              [--now UNIX_SECS] [--policy PATH] [--window SECS]
#                              [--cargo-toml PATH]
#
# --now defaults to the clock (`date +%s`); tests pass it. A live run that
# fails the check exits 1. A dry run reports the same finding as a warning and
# exits 0, so a rehearsal shows it without stopping. A policy file the script
# cannot read (a constant missing, repeated or written another way) exits 2 in
# both modes: the check cannot say anything, so it does not pass.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLICY="$SCRIPT_DIR/../crates/core/src/transport/quic/upgrade/policy.rs"
CARGO_TOML="$SCRIPT_DIR/../crates/core/Cargo.toml"
WINDOW=$((3 * 3600))
VERSION=""
LIVE=""
NOW=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version) VERSION="${2:-}"; shift 2 ;;
        --live) LIVE="${2:-}"; shift 2 ;;
        --now) NOW="${2:-}"; shift 2 ;;
        --policy) POLICY="${2:-}"; shift 2 ;;
        --window) WINDOW="${2:-}"; shift 2 ;;
        --cargo-toml) CARGO_TOML="${2:-}"; shift 2 ;;
        *) echo "::error::check_quic_data_ramp_t0: unknown argument '$1'"; exit 2 ;;
    esac
done

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::check_quic_data_ramp_t0: --version must be X.Y.Z (got '$VERSION')"
    exit 2
fi
if [[ "$LIVE" != "true" && "$LIVE" != "false" ]]; then
    echo "::error::check_quic_data_ramp_t0: --live must be true or false (got '$LIVE')"
    exit 2
fi
if [[ -z "$NOW" ]]; then
    NOW="$(date +%s)"
fi
if [[ ! "$NOW" =~ ^[0-9]+$ || ! "$WINDOW" =~ ^[0-9]+$ ]]; then
    echo "::error::check_quic_data_ramp_t0: --now and --window must be whole seconds"
    exit 2
fi
if [[ ! -f "$POLICY" ]]; then
    echo "::error::check_quic_data_ramp_t0: policy file not found: $POLICY"
    exit 2
fi

# Sets DECLARED to the value of the one declaration of constant $1 whose type
# is $2, or exits 2. The declaration starts a line and runs to its `;`,
# possibly over several lines (rustfmt wraps a long one), and its whitespace
# is collapsed, so `Some(\n    1_800_000_000,\n)` reads as `Some(1_800_000_000)`.
# Starting a line keeps a comment or test that mentions the name from being
# read; two declarations are refused rather than one picked. (Not a command
# substitution: its error message would be captured.)
DECLARED=""
read_declared() {
    local name="$1" type="$2" found count
    found="$(awk -v head="pub(in crate::transport) const ${name}: ${type} =" '
        index($0, head) == 1 { inside = 1; text = "" }
        inside { text = text " " $0; if ($0 ~ /;[[:space:]]*$/) { print text; inside = 0 } }
    ' "$POLICY")"
    count="$(printf '%s' "$found" | grep -c . || true)"
    if [[ "$count" != "1" ]]; then
        echo "::error::check_quic_data_ramp_t0: expected one declaration of ${name}: ${type} in $POLICY, found ${count}"
        exit 2
    fi
    found="${found#*= }"
    found="${found%;*}"
    # Collapse whitespace, then drop it inside the parentheses and a trailing
    # comma rustfmt adds to a wrapped argument.
    found="$(printf '%s' "$found" | tr -s '[:space:]' ' ')"
    found="${found// /}"
    found="${found/,)/)}"
    DECLARED="$found"
}

read_declared QUIC_DATA_RAMP_RELEASE 'Option<&str>'
release_value="$DECLARED"
read_declared QUIC_DATA_RAMP_START 'Option<u64>'
start_value="$DECLARED"

# Every finding below fails a live run and warns in a dry run.
finding() {
    if [[ "$LIVE" == "true" ]]; then
        echo "::error title=QUIC data ramp start::$1"
        exit 1
    fi
    echo "::warning title=QUIC data ramp start (dry run)::$1"
    exit 0
}

# Whether crate $1 version $2 is on crates.io: returns 0 published, 1 absent
# (HTTP 404), 2 unknown (any other answer, or none). Same form as
# scripts/RELEASE_RECOVERY.md's `published`: crates.io answers 403 to a request
# with no descriptive User-Agent, and only a 404 means absent.
published() {
    local code
    code="$(curl -sS -o /dev/null -w '%{http_code}' -A 'freenet-release-check' \
        --max-time 30 --retry 3 --retry-all-errors \
        "https://crates.io/api/v1/crates/$1/$2")" || return 2
    case "$code" in
        200) return 0 ;;
        404) return 1 ;;
        *) echo "crates.io answered HTTP $code for $1 $2"; return 2 ;;
    esac
}

# GET https://api.github.com/repos/freenet/freenet-core/$1 into the file $2;
# prints the HTTP status, or returns non-zero when there was no answer.
# Authenticated when a token is at hand (GH_TOKEN, GITHUB_TOKEN, or `gh auth
# token`), since the anonymous limit is 60 requests an hour per address. The
# token goes to curl in a file in a private directory (mktemp), removed on
# return or interruption, never on curl's command line.
github_get() {
    local token
    token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
    if [[ -z "$token" ]] && command -v gh >/dev/null 2>&1; then
        token="$(gh auth token 2>/dev/null || true)"
    fi
    local auth=()
    if [[ -n "$token" ]]; then
        GITHUB_GET_DIR="$(mktemp -d)"
        trap 'rm -rf "$GITHUB_GET_DIR"' EXIT INT TERM
        printf 'Authorization: Bearer %s\n' "$token" > "$GITHUB_GET_DIR/auth"
        auth=(-H "@$GITHUB_GET_DIR/auth")
    fi
    local rc=0
    # `${auth[@]+...}`: an empty array is "unbound" under `set -u` in bash
    # before 4.4 (macOS's /bin/bash).
    curl -sS -o "$2" -w '%{http_code}' -A 'freenet-release-check' \
        -H 'Accept: application/vnd.github+json' ${auth[@]+"${auth[@]}"} \
        --max-time 30 --retry 3 --retry-all-errors \
        "https://api.github.com/repos/freenet/freenet-core/$1" || rc=$?
    if [[ -n "${GITHUB_GET_DIR:-}" ]]; then
        rm -rf "$GITHUB_GET_DIR"
        GITHUB_GET_DIR=""
        trap - EXIT INT TERM
    fi
    return "$rc"
}

# Whether v$1 has reached nodes through GitHub: returns 0 yes, 1 no, 2 unknown
# (any answer other than the ones below, or none). Nodes update from GitHub's
# latest release, so v$1 must be a published (non-draft) release (404 from
# releases/tags: none, or a draft, which that endpoint does not return), and
# /releases/latest must be at v$1 or later: a release un-drafted while
# "latest" stayed pinned to an older build never reached nodes either.
github_release_reached_nodes() {
    local code body answer=2 latest
    body="$(mktemp)"
    if code="$(github_get "releases/tags/v$1" "$body")"; then
        case "$code" in
            200)
                # The endpoint returns published releases only; require the
                # body to say so too, so an answer that is not that does not
                # pass.
                if [[ "$(tr -d '[:space:]' < "$body")" == *'"draft":false'* ]]; then
                    answer=0
                else
                    echo "GitHub answered 200 for v$1 without \"draft\": false"
                fi ;;
            404) answer=1 ;;
            *) echo "GitHub answered HTTP $code for v$1" ;;
        esac
    fi
    if [[ "$answer" == 0 ]]; then
        answer=2
        if code="$(github_get "releases/latest" "$body")"; then
            if [[ "$code" != 200 ]]; then
                echo "GitHub answered HTTP $code for the latest release"
            elif [[ "$(tr -d '[:space:]' < "$body")" =~ \"tag_name\":\"v([0-9]+\.[0-9]+\.[0-9]+)\" ]]; then
                latest="${BASH_REMATCH[1]}"
                if [[ "$(printf '%s\n%s\n' "$1" "$latest" | sort -V | tail -1)" == "$latest" ]]; then
                    answer=0
                else
                    echo "GitHub's latest release is v$latest, before v$1"
                    answer=1
                fi
            else
                echo "GitHub's latest release has no vX.Y.Z tag_name"
            fi
        fi
    fi
    rm -f "$body"
    return "$answer"
}

# T0 in seconds, or empty for None; a value written another way exits 2.
t0=""
if [[ "$start_value" != "None" ]]; then
    if [[ ! "$start_value" =~ ^Some\(([0-9_]+)\)$ ]]; then
        echo "::error::check_quic_data_ramp_t0: QUIC_DATA_RAMP_START must be None or Some(<unix seconds>) (got $start_value)"
        exit 2
    fi
    t0="${BASH_REMATCH[1]//_/}"
    if [[ ! "$t0" =~ ^[0-9]+$ ]]; then
        echo "::error::check_quic_data_ramp_t0: QUIC_DATA_RAMP_START has no digits (got $start_value)"
        exit 2
    fi
    t0=$((10#$t0))
fi

if [[ "$release_value" == "None" ]]; then
    if [[ -n "$t0" ]]; then
        # Fail closed: a ramp in the tree with no release named for it would
        # ship under whatever version is cut, unchecked.
        finding "QUIC_DATA_RAMP_START is set but QUIC_DATA_RAMP_RELEASE is None, so release $VERSION would ship the ramp unchecked. Name the ramp release beside T0 in a reviewed PR (docs/RELEASING.md)."
    fi
    echo "QUIC data ramp: no ramp release (QUIC_DATA_RAMP_RELEASE = None); nothing to check."
    exit 0
fi
if [[ ! "$release_value" =~ ^Some\(\"([0-9]+\.[0-9]+\.[0-9]+)\"\)$ ]]; then
    echo "::error::check_quic_data_ramp_t0: QUIC_DATA_RAMP_RELEASE must be None or Some(\"X.Y.Z\") (got $release_value)"
    exit 2
fi
ramp_release="${BASH_REMATCH[1]}"
if [[ "$ramp_release" != "$VERSION" ]]; then
    if [[ -z "$t0" ]]; then
        # The emergency stop: T0 set back to None, the release left named.
        echo "QUIC data ramp: no ramp start (QUIC_DATA_RAMP_START = None); release $VERSION is not checked."
        exit 0
    fi
    earlier="$(printf '%s\n%s\n' "$VERSION" "$ramp_release" | sort -V | head -1)"
    # The tree's own version: the last release at dispatch (before the bump),
    # the cut version at the tag.
    if [[ ! -f "$CARGO_TOML" ]]; then
        echo "::error::check_quic_data_ramp_t0: Cargo.toml not found: $CARGO_TOML"
        exit 2
    fi
    tree_version="$(sed -n 's/^version[[:space:]]*=[[:space:]]*"\([0-9][0-9.]*\)".*/\1/p' "$CARGO_TOML" | head -1)"
    if [[ ! "$tree_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "::error::check_quic_data_ramp_t0: no X.Y.Z version in $CARGO_TOML (got '$tree_version')"
        exit 2
    fi
    if [[ "$earlier" == "$VERSION" ]]; then
        # Fail closed: the ramp is in the tree and this version comes before
        # the one named for it, so it would ship the ramp under another
        # version (an auto-bump, or the name set ahead of the cut).
        finding "Release $VERSION comes before the QUIC data ramp release ($ramp_release) but carries the ramp (QUIC_DATA_RAMP_START is set), so it would ship the ramp unchecked. Cut the ramp release as $ramp_release, or change QUIC_DATA_RAMP_RELEASE in the same reviewed PR as T0 (docs/RELEASING.md)."
    fi
    reached="$(printf '%s\n%s\n' "$ramp_release" "$tree_version" | sort -V | tail -1)"
    if [[ "$reached" != "$tree_version" ]]; then
        # Fail closed: the ramp release has not been cut from this tree yet,
        # and this later version skips it (an auto-bump past a taken number).
        finding "Release $VERSION skips the QUIC data ramp release ($ramp_release), which this tree ($tree_version) has not reached, and carries the ramp (QUIC_DATA_RAMP_START is set), so it would ship the ramp unchecked. Cut $ramp_release, or change QUIC_DATA_RAMP_RELEASE in the same reviewed PR as T0 (docs/RELEASING.md)."
    fi
    # The tree reaching the ramp release says only that its bump merged; its
    # publish can still have failed or been cancelled, and then this later
    # version would be the first to ship the ramp, unchecked. So the ramp
    # release must be a published GitHub release (what nodes update from) and
    # on crates.io, and an answer that is not a clear yes or no fails closed.
    github_release_reached_nodes "$ramp_release"
    case $? in
        0) ;;
        1) finding "Release $VERSION comes after the QUIC data ramp release ($ramp_release), but v$ramp_release has not reached nodes (it is not a published GitHub release, or GitHub's latest release is before it), so this release would ship the ramp unchecked. Publish v$ramp_release and make it latest first (scripts/RELEASE_RECOVERY.md), or change QUIC_DATA_RAMP_RELEASE and T0 in a reviewed PR (docs/RELEASING.md)." ;;
        *) finding "Release $VERSION comes after the QUIC data ramp release ($ramp_release), and whether v$ramp_release has reached nodes through GitHub cannot be determined, so this release is refused rather than risk shipping the ramp unchecked. Retry when GitHub answers." ;;
    esac
    published freenet "$ramp_release"
    case $? in
        0) ;;
        1) finding "Release $VERSION comes after the QUIC data ramp release ($ramp_release), but freenet $ramp_release is not on crates.io: the ramp release was not published, so this release would ship the ramp unchecked. Publish $ramp_release first (scripts/RELEASE_RECOVERY.md), or change QUIC_DATA_RAMP_RELEASE and T0 in a reviewed PR (docs/RELEASING.md)." ;;
        *) finding "Release $VERSION comes after the QUIC data ramp release ($ramp_release), and whether freenet $ramp_release is on crates.io cannot be determined, so this release is refused rather than risk shipping the ramp unchecked. Retry when crates.io answers." ;;
    esac
    if (( NOW + WINDOW < t0 )); then
        # A later version cut before T0 would start the ramp at 5% early.
        finding "Release $VERSION comes after the QUIC data ramp release ($ramp_release) but is cut more than $((WINDOW / 60)) min before T0 ($t0), so it would ship the ramp ahead of its start. Re-set T0 or the ramp release in a reviewed PR (docs/RELEASING.md)."
    fi
    echo "::warning title=QUIC data ramp start::Release $VERSION comes after the QUIC data ramp release ($ramp_release) and carries its ramp start T0 ($t0); its timing is not checked. If this release starts a new ramp, QUIC_DATA_RAMP_RELEASE must name it."
    exit 0
fi

# This is the ramp release.
if [[ -z "$t0" ]]; then
    finding "Release $VERSION is the QUIC data ramp release (QUIC_DATA_RAMP_RELEASE), but QUIC_DATA_RAMP_START is None. To cut the ramp, set T0 to the planned publish time in a reviewed PR first. After an emergency stop, cut another version instead, or set QUIC_DATA_RAMP_RELEASE to None in a reviewed PR (docs/RELEASING.md)."
fi
off=$((NOW - t0))
abs=${off#-}
if (( abs > WINDOW )); then
    if (( off > 0 )); then
        where="$((abs / 60)) min after"
    else
        where="$((abs / 60)) min before"
    fi
    finding "Release $VERSION is the QUIC data ramp release, and now ($NOW) is $where its ramp start T0 ($t0), past the $((WINDOW / 60)) min window. Re-set QUIC_DATA_RAMP_START to the planned publish time in a new reviewed PR before publishing (docs/RELEASING.md)."
fi
echo "QUIC data ramp: release $VERSION is the ramp release; now ($NOW) is within $((WINDOW / 60)) min of T0 ($t0), off by $((off / 60)) min."
exit 0
